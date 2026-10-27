// Command traincache 是 TrainCache CLI:纯 gRPC 客户端(design §5.6;D20)。
// 不直接操作缓存目录,不重复实现索引/下载/落盘逻辑。
//
// 子命令:
//
//	traincache prefetch --path PATH --node NODE (--chunks PATH:CHUNK,... | --files TAR,...) [--dataset NAME] [--snapshot-id ID] [--priority p0|p1|p2|p3]
//	traincache node status --node NODE [--item agent|catalog|space|tasks|rescan]
//	traincache status --node NODE --task-id ID
//	traincache ls --dataset NAME [--snapshot-id ID] [--prefix P] [--chunks]
//
// 退出码:0 成功;1 RPC/运行错误;2 参数错误(usage)。
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"sort"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	agentpb "traincache/agent/api/agentpb"
)

const (
	// defaultEndpoint 是全局 --endpoint 默认值(D20);直连模式(Q2 结案:开发态先行)。
	defaultEndpoint = "127.0.0.1:9410"
	// rpcTimeout 是每 RPC 的 context 超时(design §5.1:默认 10s)。
	rpcTimeout = 10 * time.Second
)

// 退出码。
const (
	exitOK    = 0 // 成功
	exitError = 1 // RPC/运行错误
	exitUsage = 2 // 参数错误
)

func main() {
	os.Exit(run(os.Args[1:]))
}

func run(args []string) int {
	if len(args) == 0 {
		printUsage(os.Stderr)
		return exitUsage
	}
	switch args[0] {
	case "prefetch":
		return runPrefetch(args[1:])
	case "node":
		return runNode(args[1:])
	case "status":
		return runStatus(args[1:])
	case "ls":
		return runLS(args[1:])
	case "help", "-h", "--help":
		printUsage(os.Stdout)
		return exitOK
	default:
		fmt.Fprintf(os.Stderr, "traincache: 未知子命令 %q\n", args[0])
		printUsage(os.Stderr)
		return exitUsage
	}
}

func printUsage(w io.Writer) {
	fmt.Fprint(w, `用法:
  traincache prefetch --path PATH --node NODE (--chunks PATH:CHUNK,... | --files TAR,...) [--dataset NAME] [--snapshot-id ID] [--priority p0|p1|p2|p3]
  traincache node status --node NODE [--item agent|catalog|space|tasks|rescan]
  traincache status --node NODE --task-id ID
  traincache ls --dataset NAME [--snapshot-id ID] [--prefix P] [--chunks]

全局选项:
  --endpoint ADDR   Agent gRPC 地址(默认 127.0.0.1:9410;--node 仅作请求校验字段,不做地址解析)
`)
}

// runPrefetch 实现 `traincache prefetch`(design §5.6)。
func runPrefetch(args []string) int {
	fs := flag.NewFlagSet("traincache prefetch", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	endpoint := fs.String("endpoint", defaultEndpoint, "Agent gRPC 地址(全局选项)")
	taskID := fs.String("task-id", "", "预热任务 ID(幂等键,必填)")
	path := fs.String("path", "", "数据路径前缀(必填)")
	node := fs.String("node", "", "目标节点(必填;仅作请求校验字段)")
	chunks := fs.String("chunks", "", "精确块清单,逗号分隔,格式 path:chunk")
	files := fs.String("files", "", "tar 分片名清单,逗号分隔;经 ListSnapshot 展开为块")
	dataset := fs.String("dataset", "", "dataset 名(可选)")
	snapshotID := fs.String("snapshot-id", "", "快照 ID(可选,空=当前 CURRENT)")
	priority := fs.String("priority", "p0", "预取优先级 p0|p1|p2|p3")
	if err := fs.Parse(args); err != nil {
		return parseExitCode(err)
	}
	if fs.NArg() > 0 {
		return usageError(fs, "prefetch: 不接受位置参数 %q", fs.Args())
	}
	if *taskID == "" || *path == "" || *node == "" {
		return usageError(fs, "prefetch: --task-id 与 --path 与 --node 必填")
	}
	chunkSpecs := splitCSV(*chunks)
	fileNames := splitCSV(*files)
	if len(chunkSpecs) > 0 && len(fileNames) > 0 {
		return usageError(fs, "prefetch: --chunks 与 --files 只能给一个")
	}
	if len(chunkSpecs) == 0 && len(fileNames) == 0 {
		return usageError(fs, "prefetch: --chunks 或 --files 至少给一个")
	}
	if len(fileNames) > 0 && *dataset == "" {
		return usageError(fs, "prefetch: --files 需要 --dataset 才能展开分片")
	}
	prio, ok := parsePriority(*priority)
	if !ok {
		return usageError(fs, "prefetch: --priority=%q 非法(仅支持 p0|p1|p2|p3)", *priority)
	}

	conn, err := dial(*endpoint)
	if err != nil {
		fmt.Fprintf(os.Stderr, "traincache: 建立连接失败: %v\n", err)
		return exitError
	}
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), rpcTimeout)
	defer cancel()

	var chunkRefs []*agentpb.ChunkRef
	if len(fileNames) > 0 {
		// --files 先按分片名查快照,再展开为该分片的全部块;Prefix 留空,
		// 因为这里匹配的是分片名而不是数据路径前缀。
		listResp, err := agentpb.NewNodeServiceClient(conn).ListSnapshot(ctx, &agentpb.ListSnapshotRequest{
			Dataset:    *dataset,
			SnapshotId: *snapshotID,
		})
		if err != nil {
			return rpcError(err, "prefetch: ListSnapshot 失败")
		}
		var missing []string
		chunkRefs, missing = expandFileChunks(listResp.GetEntries(), fileNames)
		if len(missing) > 0 {
			fmt.Fprintf(os.Stderr, "traincache: prefetch: 快照中找不到分片 %s\n", strings.Join(missing, ","))
			return exitError
		}
	} else {
		var err error
		chunkRefs, err = parseChunkSpecs(chunkSpecs)
		if err != nil {
			return usageError(fs, "prefetch: %v", err)
		}
	}

	resp, err := agentpb.NewPreheatServiceClient(conn).SubmitPreheat(ctx, &agentpb.SubmitPreheatRequest{
		TaskId:     *taskID,
		Node:       *node,
		Dataset:    *dataset,
		SnapshotId: *snapshotID,
		Prefixes:   []string{*path}, // --path 即路径前缀(SubmitPreheatRequest.prefixes,§5.1)
		Chunks:     chunkRefs,
		Priority:   prio,
	})
	if err != nil {
		return rpcError(err, "prefetch: 提交预热任务失败")
	}
	fmt.Printf("accepted=%t task_id=%s %s\n", resp.GetAccepted(), resp.GetTaskId(), resp.GetMessage())
	return exitOK
}

// runNode 实现 `traincache node` 子命令族(design §5.6 当前仅 status)。
func runNode(args []string) int {
	if len(args) == 0 {
		fmt.Fprintln(os.Stderr, "traincache: node 需要子命令(status)")
		printUsage(os.Stderr)
		return exitUsage
	}
	switch args[0] {
	case "status":
		return runNodeStatus(args[1:])
	case "help", "-h", "--help":
		printUsage(os.Stdout)
		return exitOK
	default:
		fmt.Fprintf(os.Stderr, "traincache: 未知 node 子命令 %q(仅支持 status)\n", args[0])
		return exitUsage
	}
}

// runNodeStatus 实现 `traincache node status`(design §5.6)。
func runNodeStatus(args []string) int {
	fs := flag.NewFlagSet("traincache node status", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	endpoint := fs.String("endpoint", defaultEndpoint, "Agent gRPC 地址(全局选项)")
	node := fs.String("node", "", "目标节点(必填;仅作请求校验字段)")
	item := fs.String("item", "", "只查一项:agent|catalog|space|tasks|rescan(缺省=全量)")
	if err := fs.Parse(args); err != nil {
		return parseExitCode(err)
	}
	if fs.NArg() > 0 {
		return usageError(fs, "node status: 不接受位置参数 %q", fs.Args())
	}
	if *node == "" {
		return usageError(fs, "node status: --node 必填")
	}
	itemValue, ok := parseItem(*item)
	if !ok {
		return usageError(fs, "node status: --item=%q 非法(仅支持 agent|catalog|space|tasks|rescan)", *item)
	}

	conn, err := dial(*endpoint)
	if err != nil {
		fmt.Fprintf(os.Stderr, "traincache: 建立连接失败: %v\n", err)
		return exitError
	}
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), rpcTimeout)
	defer cancel()

	resp, err := agentpb.NewNodeServiceClient(conn).GetNodeStatus(ctx, &agentpb.GetNodeStatusRequest{
		Node: *node,
		Item: itemValue,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "traincache: GetNodeStatus 失败: %v\n", err)
		return exitError
	}
	printNodeStatus(resp, itemValue)
	return exitOK
}

// runStatus 实现 `traincache status`(design §5.6)。
func runStatus(args []string) int {
	fs := flag.NewFlagSet("traincache status", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	endpoint := fs.String("endpoint", defaultEndpoint, "Agent gRPC 地址(全局选项)")
	node := fs.String("node", "", "目标节点(必填;仅作请求校验字段)")
	taskID := fs.String("task-id", "", "预热任务 ID(必填)")
	if err := fs.Parse(args); err != nil {
		return parseExitCode(err)
	}
	if fs.NArg() > 0 {
		return usageError(fs, "status: 不接受位置参数 %q", fs.Args())
	}
	if *node == "" || *taskID == "" {
		return usageError(fs, "status: --node 与 --task-id 必填")
	}

	conn, err := dial(*endpoint)
	if err != nil {
		fmt.Fprintf(os.Stderr, "traincache: 建立连接失败: %v\n", err)
		return exitError
	}
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), rpcTimeout)
	defer cancel()

	resp, err := agentpb.NewPreheatServiceClient(conn).GetPreheatStatus(ctx, &agentpb.GetPreheatStatusRequest{
		TaskId: *taskID,
	})
	if err != nil {
		return rpcError(err, "status 未实现(里程碑 M3)")
	}
	printPreheatStatus(resp)
	return exitOK
}

// snapshotLister 是 ListSnapshot 的最小客户端接口(agentpb.NodeServiceClient 实现),
// 抽出接口便于测试注入 fake,不连真实 Agent。
type snapshotLister interface {
	ListSnapshot(ctx context.Context, in *agentpb.ListSnapshotRequest, opts ...grpc.CallOption) (*agentpb.ListSnapshotResponse, error)
}

// runLS 实现 `traincache ls`(design §5.6;只读列快照)。
func runLS(args []string) int {
	return lsSnapshot(args, os.Stdout, os.Stderr, dialSnapshot)
}

// lsSnapshot 是 runLS 的可测形式:输出流与客户端构造器由调用方注入。
func lsSnapshot(args []string, stdout, stderr io.Writer, newClient func(string) (snapshotLister, func(), error)) int {
	fs := flag.NewFlagSet("traincache ls", flag.ContinueOnError)
	fs.SetOutput(stderr)
	endpoint := fs.String("endpoint", defaultEndpoint, "Agent gRPC 地址(全局选项)")
	dataset := fs.String("dataset", "", "dataset 名(必填)")
	snapshotID := fs.String("snapshot-id", "", "快照 ID(空=当前 CURRENT)")
	prefix := fs.String("prefix", "", "路径前缀(空=不过滤)")
	byChunk := fs.Bool("chunks", false, "展开到块级明细(默认按文件聚合)")
	if err := fs.Parse(args); err != nil {
		return parseExitCode(err)
	}
	if fs.NArg() > 0 {
		return usageError(fs, "ls: 不接受位置参数 %q", fs.Args())
	}
	if *dataset == "" {
		return usageError(fs, "ls: --dataset 必填")
	}

	client, closeClient, err := newClient(*endpoint)
	if err != nil {
		fmt.Fprintf(stderr, "traincache: 建立连接失败: %v\n", err)
		return exitError
	}
	defer closeClient()

	ctx, cancel := context.WithTimeout(context.Background(), rpcTimeout)
	defer cancel()

	resp, err := client.ListSnapshot(ctx, &agentpb.ListSnapshotRequest{
		Dataset:    *dataset,
		SnapshotId: *snapshotID,
		Prefix:     *prefix,
	})
	if err != nil {
		fmt.Fprintf(stderr, "traincache: ListSnapshot 失败: %v\n", err)
		return exitError
	}
	if err := writeListSnapshot(stdout, resp.GetEntries(), *byChunk); err != nil {
		fmt.Fprintf(stderr, "traincache: 输出失败: %v\n", err)
		return exitError
	}
	return exitOK
}

// dialSnapshot 建立 gRPC 连接并返回 ListSnapshot 客户端与关闭函数。
func dialSnapshot(endpoint string) (snapshotLister, func(), error) {
	conn, err := dial(endpoint)
	if err != nil {
		return nil, nil, err
	}
	return agentpb.NewNodeServiceClient(conn), func() { _ = conn.Close() }, nil
}

// fileSummary 是一个 path 的聚合结果:块数与字节合计。
type fileSummary struct {
	path   string
	chunks int64
	bytes  int64
}

// chunkSummary 是一个块的明细行。
type chunkSummary struct {
	path  string
	chunk int64
	bytes int64
}

// summarizeFiles 按 path 合并条目:块数计数、字节求和,结果按 path 升序。
func summarizeFiles(entries []*agentpb.EntryRef) []fileSummary {
	byPath := make(map[string]*fileSummary, len(entries))
	for _, e := range entries {
		s, ok := byPath[e.GetPath()]
		if !ok {
			s = &fileSummary{path: e.GetPath()}
			byPath[e.GetPath()] = s
		}
		s.chunks++
		s.bytes += e.GetSize()
	}
	out := make([]fileSummary, 0, len(byPath))
	for _, s := range byPath {
		out = append(out, *s)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].path < out[j].path })
	return out
}

// expandChunks 展开块级条目,按 (path, chunk) 升序。
func expandChunks(entries []*agentpb.EntryRef) []chunkSummary {
	out := make([]chunkSummary, 0, len(entries))
	for _, e := range entries {
		out = append(out, chunkSummary{path: e.GetPath(), chunk: e.GetChunk(), bytes: e.GetSize()})
	}
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].path != out[j].path {
			return out[i].path < out[j].path
		}
		return out[i].chunk < out[j].chunk
	})
	return out
}

// writeListSnapshot 输出 ls 表格:默认 FILE/CHUNKS/BYTES 文件级聚合,
// byChunk 时 FILE/CHUNK/BYTES 块级明细;空结果只输出表头。
func writeListSnapshot(w io.Writer, entries []*agentpb.EntryRef, byChunk bool) error {
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	if byChunk {
		fmt.Fprintln(tw, "FILE\tCHUNK\tBYTES")
		for _, c := range expandChunks(entries) {
			fmt.Fprintf(tw, "%s\t%d\t%d\n", c.path, c.chunk, c.bytes)
		}
	} else {
		fmt.Fprintln(tw, "FILE\tCHUNKS\tBYTES")
		for _, f := range summarizeFiles(entries) {
			fmt.Fprintf(tw, "%s\t%d\t%d\n", f.path, f.chunks, f.bytes)
		}
	}
	return tw.Flush()
}

// printNodeStatus 输出两列表格(design §5.6;Q9 结案:默认表格);--item 只显示对应行。
func printNodeStatus(resp *agentpb.NodeStatus, item agentpb.NodeStatusItem) {
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	defer w.Flush()

	row := func(k, v string) { fmt.Fprintf(w, "%s\t%s\n", k, v) }
	row("NODE", resp.GetNode())
	switch item {
	case agentpb.NodeStatusItem_UNSPECIFIED:
		row("AGENT", strconv.FormatBool(resp.GetAgentOk()))
		row("CACHE_ROOT", resp.GetCacheRoot())
		row("CACHED_BYTES", strconv.FormatInt(resp.GetCachedBytes(), 10))
		row("FREE_BYTES", strconv.FormatInt(resp.GetFreeBytes(), 10))
		row("RUNNING_TASKS", strconv.FormatInt(resp.GetRunningTasks(), 10))
		row("RESCAN_PHASE", rescanPhaseText(resp.GetRescan()))
	case agentpb.NodeStatusItem_AGENT:
		row("AGENT", strconv.FormatBool(resp.GetAgentOk()))
	case agentpb.NodeStatusItem_CATALOG:
		for _, c := range resp.GetCatalog() {
			row("CATALOG", fmt.Sprintf("%s/%s shards=%d entries=%d",
				c.GetDataset(), c.GetSnapshotId(), c.GetShards(), c.GetEntries()))
		}
	case agentpb.NodeStatusItem_SPACE:
		row("CACHE_ROOT", resp.GetCacheRoot())
		row("CACHED_BYTES", strconv.FormatInt(resp.GetCachedBytes(), 10))
		row("FREE_BYTES", strconv.FormatInt(resp.GetFreeBytes(), 10))
	case agentpb.NodeStatusItem_TASKS:
		row("RUNNING_TASKS", strconv.FormatInt(resp.GetRunningTasks(), 10))
	case agentpb.NodeStatusItem_RESCAN:
		row("RESCAN_PHASE", rescanPhaseText(resp.GetRescan()))
	}
}

// printPreheatStatus 输出任务状态表(design §5.6:任务 id、目标节点、数据路径、任务状态、
// 总块数/已完成/失败、开始时间、最近更新时间)。
func printPreheatStatus(st *agentpb.PreheatStatus) {
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	defer w.Flush()

	row := func(k, v string) { fmt.Fprintf(w, "%s\t%s\n", k, v) }
	row("TASK_ID", st.GetTaskId())
	row("NODE", st.GetNode())
	row("PATHS", strings.Join(st.GetPaths(), ","))
	row("STATE", st.GetState().String()) // QUEUED / RUNNING / DONE / ERROR(§5.1 冻结枚举)
	row("TOTAL_BLOCKS", strconv.FormatInt(st.GetTotalBlocks(), 10))
	row("COMPLETED_BLOCKS", strconv.FormatInt(st.GetCompletedBlocks(), 10))
	row("FAILED_BLOCKS", strconv.FormatInt(st.GetFailedBlocks(), 10))
	row("STARTED_AT", timestampText(st.GetStartedAt()))
	row("UPDATED_AT", timestampText(st.GetUpdatedAt()))
}

// dial 建立到 Agent 的 gRPC 连接(惰性连接,首次 RPC 才拨号);开发态明文(Q2)。
func dial(endpoint string) (*grpc.ClientConn, error) {
	return grpc.NewClient(endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
}

// rpcError 输出 RPC 错误:Unimplemented 打印骨架期里程碑提示,其余打印原始错误;均返回退出码 1。
func rpcError(err error, unimplementedMsg string) int {
	if status.Code(err) == codes.Unimplemented {
		fmt.Fprintln(os.Stderr, unimplementedMsg)
		return exitError
	}
	fmt.Fprintf(os.Stderr, "traincache: RPC 失败: %v\n", err)
	return exitError
}

// parseExitCode 把 FlagSet.Parse 的错误转成退出码(-h/--help 视为成功)。
func parseExitCode(err error) int {
	if errors.Is(err, flag.ErrHelp) {
		return exitOK
	}
	return exitUsage // flag 包已打印错误与用法
}

// usageError 打印参数错误与子命令用法,返回退出码 2。
func usageError(fs *flag.FlagSet, format string, args ...any) int {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	fs.Usage()
	return exitUsage
}

// parsePriority 映射 --priority 到冻结枚举(默认 P0)。
func parsePriority(s string) (agentpb.PreheatPriority, bool) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "p0":
		return agentpb.PreheatPriority_P0, true
	case "p1":
		return agentpb.PreheatPriority_P1, true
	case "p2":
		return agentpb.PreheatPriority_P2, true
	case "p3":
		return agentpb.PreheatPriority_P3, true
	default:
		return agentpb.PreheatPriority_P0, false
	}
}

// parseItem 映射 --item 到冻结枚举;空串 = UNSPECIFIED(全量)。
func parseItem(s string) (agentpb.NodeStatusItem, bool) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "":
		return agentpb.NodeStatusItem_UNSPECIFIED, true
	case "agent":
		return agentpb.NodeStatusItem_AGENT, true
	case "catalog":
		return agentpb.NodeStatusItem_CATALOG, true
	case "space":
		return agentpb.NodeStatusItem_SPACE, true
	case "tasks":
		return agentpb.NodeStatusItem_TASKS, true
	case "rescan":
		return agentpb.NodeStatusItem_RESCAN, true
	default:
		return agentpb.NodeStatusItem_UNSPECIFIED, false
	}
}

// expandFileChunks 按 tar 分片名收集快照条目,输出顺序为 files 的给出顺序、
// 文件内按 chunk 升序。未命中的分片名单独返回。
func expandFileChunks(entries []*agentpb.EntryRef, files []string) (refs []*agentpb.ChunkRef, missing []string) {
	wanted := make(map[string]struct{}, len(files))
	order := make([]string, 0, len(files))
	for _, name := range files {
		if _, ok := wanted[name]; ok {
			continue
		}
		wanted[name] = struct{}{}
		order = append(order, name)
	}
	byFile := make(map[string][]*agentpb.EntryRef, len(order))
	for _, e := range entries {
		if _, ok := wanted[e.GetPath()]; ok {
			byFile[e.GetPath()] = append(byFile[e.GetPath()], e)
		}
	}
	for _, name := range order {
		items := byFile[name]
		if len(items) == 0 {
			missing = append(missing, name)
			continue
		}
		sort.Slice(items, func(i, j int) bool { return items[i].GetChunk() < items[j].GetChunk() })
		for _, e := range items {
			refs = append(refs, &agentpb.ChunkRef{Path: e.GetPath(), Chunk: e.GetChunk()})
		}
	}
	return refs, missing
}

// parseChunkSpecs 解析 path:chunk 形式的精确块清单。
func parseChunkSpecs(items []string) ([]*agentpb.ChunkRef, error) {
	refs := make([]*agentpb.ChunkRef, 0, len(items))
	for _, item := range items {
		i := strings.LastIndex(item, ":")
		if i <= 0 || i == len(item)-1 {
			return nil, fmt.Errorf("块 %q 非法(需为 path:chunk)", item)
		}
		chunk, err := strconv.ParseInt(item[i+1:], 10, 64)
		if err != nil || chunk < 0 {
			return nil, fmt.Errorf("块 %q 的 chunk 非非负整数", item)
		}
		refs = append(refs, &agentpb.ChunkRef{Path: item[:i], Chunk: chunk})
	}
	return refs, nil
}

// splitCSV 逗号切分、TrimSpace、丢空项(--chunks 校验口径,M1 不解析名字)。
func splitCSV(s string) []string {
	var out []string
	for _, part := range strings.Split(s, ",") {
		if p := strings.TrimSpace(part); p != "" {
			out = append(out, p)
		}
	}
	return out
}

// rescanPhaseText 渲染 rescan.phase;rescan 缺省(unset)显示 "-"。
func rescanPhaseText(r *agentpb.RescanStatus) string {
	if r == nil {
		return "-"
	}
	return r.GetPhase().String()
}

// timestampText 渲染 protobuf 时间戳为 RFC3339;缺省/非法显示 "-"。
func timestampText(ts *timestamppb.Timestamp) string {
	if ts == nil || !ts.IsValid() {
		return "-"
	}
	return ts.AsTime().Format(time.RFC3339)
}
