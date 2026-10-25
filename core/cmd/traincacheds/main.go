// Command traincacheds 是数据集管理 CLI（design.md §11，M13a 只读半边）。
//
// 本步只提供查询面：ls datasets / ls snapshots / ls entries / stat，全部只读
// 索引侧对象（CURRENT、manifest.json、索引分片），绝不构造 cas.Store 或
// index.Writer，也绝不发出 Put/PutIfMatch/Delete。删除与回收（rm/gc）属后续
// 里程碑，本 CLI 显式拒绝并把它们当用法错误报出，免得脚本以为已经生效。
//
// 对象存储参数与 trainpublish 同名同口径：flag 显式值优先，未显式指定时取
// TRAINCACHE_OBJSTORE_* 环境变量族。退出码：0 成功；1 运行期错误（对象存储、
// 无 CURRENT、CURRENT/manifest 损坏等）；2 参数校验失败。
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"

	"traincache/core/index"
	"traincache/core/objectstore"
)

// 对象存储相关环境变量与默认值，与 trainpublish/agent-svc 同名同口径。
const (
	envObjstoreEndpoint     = "TRAINCACHE_OBJSTORE_ENDPOINT"
	envObjstoreAccessKey    = "TRAINCACHE_OBJSTORE_ACCESS_KEY"
	envObjstoreSecretKey    = "TRAINCACHE_OBJSTORE_SECRET_KEY"
	envObjstoreBucket       = "TRAINCACHE_OBJSTORE_BUCKET"
	envObjstoreSecure       = "TRAINCACHE_OBJSTORE_SECURE"
	defaultObjstoreEndpoint = "127.0.0.1:9000"
	defaultObjstoreBucket   = "traincache"

	programName      = "traincacheds"
	exitOK           = 0
	exitRuntimeError = 1
	exitUsageError   = 2

	// 索引侧对象键字面量（design.md §6.1 布局）：CURRENT 键走 index.CurrentKey，
	// manifest 键走 index.ManifestKey，只有目录级枚举前缀需要自己拼。
	datasetKeyPrefix  = "dataset/"
	currentKeySuffix  = "/CURRENT"
	snapshotsDirName  = "snapshots"
	manifestFileName  = "manifest.json"
	notImplementedTip = "not implemented yet (delete/gc land in a later milestone)"
)

// valueFlagNames 是必须带值的 flag 名表；其余以 "-" 开头的参数按布尔 flag 或未知
// flag 处理。Go 的 flag 包遇到第一个位置参数就停止解析，而本 CLI 的命令面是
// 「子命令 + 位置参数 + flag」（如 ls entries <ds> --prefix p/），所以先用本表把
// flag 参数与位置参数分成两段，再交给 FlagSet 解析（见 splitArgs）。
var valueFlagNames = map[string]bool{
	"endpoint":   true,
	"access-key": true,
	"secret-key": true,
	"bucket":     true,
	"prefix":     true,
	"limit":      true,
	"path":       true,
}

// deps 是可注入的构造依赖，便于测试中替换成不连网的内存 Mock。
// 只读 CLI 只有一个依赖：对象存储客户端。
type deps struct {
	newStore func(endpoint, accessKey, secretKey, bucket string, secure bool) (objectstore.Store, error)
}

// defaultDeps 返回真实实现。
func defaultDeps() deps {
	return deps{
		newStore: func(endpoint, accessKey, secretKey, bucket string, secure bool) (objectstore.Store, error) {
			return objectstore.NewS3Store(endpoint, accessKey, secretKey, bucket, secure)
		},
	}
}

// storeOptions 是构造对象存储客户端所需的全部参数。
type storeOptions struct {
	endpoint  string
	accessKey string
	secretKey string
	bucket    string
	secure    bool
}

// cli 是一次已解析完参数的调用上下文：写手、依赖与命令面选项。
type cli struct {
	ctx    context.Context
	stdout io.Writer
	stderr io.Writer
	deps   deps
	fs     *flag.FlagSet
	opts   storeOptions
	prefix string
	path   string
	limit  int
}

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr, defaultDeps()))
}

// run 执行一次 CLI 调用并返回退出码；不调用 os.Exit，便于测试。
func run(args []string, stdout, stderr io.Writer, d deps) int {
	c := &cli{ctx: context.Background(), stdout: stdout, stderr: stderr, deps: d}

	fs := flag.NewFlagSet(programName, flag.ContinueOnError)
	fs.SetOutput(stderr)
	endpoint := fs.String("endpoint", envOr(envObjstoreEndpoint, defaultObjstoreEndpoint), "object store endpoint")
	accessKey := fs.String("access-key", envOr(envObjstoreAccessKey, ""), "object store access key")
	secretKey := fs.String("secret-key", envOr(envObjstoreSecretKey, ""), "object store secret key")
	bucket := fs.String("bucket", envOr(envObjstoreBucket, defaultObjstoreBucket), "object store bucket")
	secure := fs.Bool("secure", envBool(envObjstoreSecure), "use TLS for the object store")
	prefix := fs.String("prefix", "", "ls entries: keep only entries whose path starts with this prefix")
	limit := fs.Int("limit", 0, "ls entries: keep only the first n entries (0 = unlimited)")
	path := fs.String("path", "", "stat: report totals over the entries of this path only")
	c.fs = fs
	// flag 包在解析失败与 -h 时用它打印用法，保证 stderr 里始终是本 CLI 的命令面。
	fs.Usage = func() { c.usage() }

	flagArgs, positional, err := splitArgs(args)
	if err != nil {
		reportError(stderr, err)
		c.usage()
		return exitUsageError
	}
	if err := fs.Parse(flagArgs); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return exitOK
		}
		// flag 包已把 "flag provided but not defined" 之类错误与用法写到 stderr。
		return exitUsageError
	}

	c.opts = storeOptions{endpoint: *endpoint, accessKey: *accessKey, secretKey: *secretKey, bucket: *bucket, secure: *secure}
	c.prefix = *prefix
	c.limit = *limit
	c.path = *path
	return c.execute(positional)
}

// splitArgs 按 flag 包的取值语义把参数拆成 flag 参数与位置参数两段。
//
//   - "--" 之后的全部参数原样算位置参数；
//   - "--name=value"（或 "-name=value"）自带值，直接交给 FlagSet；
//   - valueFlagNames 里的 flag 名吃掉下一个参数作为值，改写成 "--name=value"
//     交给 FlagSet（下一个参数缺失即报错，由调用方按用法错误退出 2）；
//   - 其余以 "-" 开头的参数（布尔 flag、未知 flag）原样交给 FlagSet；
//   - 不以 "-" 开头的参数是位置参数。
func splitArgs(args []string) (flagArgs, positional []string, err error) {
	for i := 0; i < len(args); i++ {
		arg := args[i]
		switch {
		case arg == "--":
			return flagArgs, append(positional, args[i+1:]...), nil
		case arg == "-" || !strings.HasPrefix(arg, "-"):
			positional = append(positional, arg)
		case strings.Contains(arg, "="), !valueFlagNames[strings.TrimLeft(arg, "-")]:
			flagArgs = append(flagArgs, arg)
		default:
			if i+1 >= len(args) {
				return nil, nil, fmt.Errorf("missing value for flag %s", arg)
			}
			flagArgs = append(flagArgs, arg+"="+args[i+1])
			i++
		}
	}
	return flagArgs, positional, nil
}

// execute 按命令面分发；返回进程退出码。
func (c *cli) execute(positional []string) int {
	if len(positional) == 0 {
		c.usage()
		return exitUsageError
	}
	switch positional[0] {
	case "ls":
		return c.executeLS(positional[1:])
	case "stat":
		return c.executeStat(positional[1:])
	case "rm", "gc":
		// 写侧命令未交付：必须报错退出，不能让脚本以为删除/回收已生效（§11.1 边界）。
		reportError(c.stderr, fmt.Errorf("subcommand %q is %s", positional[0], notImplementedTip))
		return exitUsageError
	default:
		reportError(c.stderr, fmt.Errorf("unknown command %q", positional[0]))
		c.usage()
		return exitUsageError
	}
}

// executeLS 处理 ls datasets / ls snapshots <ds> / ls entries <ds>。
func (c *cli) executeLS(rest []string) int {
	if len(rest) == 0 {
		reportError(c.stderr, errors.New("ls needs one of: datasets, snapshots <ds>, entries <ds>"))
		c.usage()
		return exitUsageError
	}
	kind := rest[0]
	if kind != "datasets" && kind != "snapshots" && kind != "entries" {
		reportError(c.stderr, fmt.Errorf("unknown ls subcommand %q", kind))
		c.usage()
		return exitUsageError
	}
	// datasets 不带位置参数；snapshots/entries 恰带一个数据集名。
	if (kind == "datasets" && len(rest) != 1) || (kind != "datasets" && len(rest) != 2) {
		reportError(c.stderr, fmt.Errorf("ls %s takes %s, got %d argument(s)", kind, arityTip(kind), len(rest)-1))
		c.usage()
		return exitUsageError
	}
	switch kind {
	case "datasets":
		if !c.allowFlags() {
			return exitUsageError
		}
		return c.withStore(func(store objectstore.Store) error { return c.lsDatasets(store) })
	case "snapshots":
		if !c.allowFlags() {
			return exitUsageError
		}
		dataset := rest[1]
		return c.withStore(func(store objectstore.Store) error { return c.lsSnapshots(store, dataset) })
	default: // entries
		if !c.allowFlags("prefix", "limit") {
			return exitUsageError
		}
		if c.limit < 0 {
			reportError(c.stderr, fmt.Errorf("--limit must be >= 0, got %d", c.limit))
			c.usage()
			return exitUsageError
		}
		dataset := rest[1]
		return c.withStore(func(store objectstore.Store) error { return c.lsEntries(store, dataset) })
	}
}

// executeStat 处理 stat <ds> [--path p.tar]。
func (c *cli) executeStat(rest []string) int {
	if len(rest) != 1 {
		reportError(c.stderr, fmt.Errorf("stat needs exactly one dataset name, got %d argument(s)", len(rest)))
		c.usage()
		return exitUsageError
	}
	if !c.allowFlags("path") {
		return exitUsageError
	}
	dataset := rest[0]
	return c.withStore(func(store objectstore.Store) error {
		if c.path != "" {
			return c.statPath(store, dataset)
		}
		return c.statDataset(store, dataset)
	})
}

// arityTip 给出 ls 子命令位置参数的期望文案（报错信息用）。
func arityTip(kind string) string {
	if kind == "datasets" {
		return "no argument"
	}
	return "exactly one dataset name"
}

// allowFlags 校验显式给出的 flag：全局对象存储 flag 恒可用，extra 是本命令额外允许的
// flag 名；出现别的 flag（如给 ls datasets 传 --prefix）即写错误与用法并返回 false
// （退出码 2 由调用方给出）。fs.Visit 只回调真正被指定的 flag，所以默认值不误报。
func (c *cli) allowFlags(extra ...string) bool {
	set := map[string]bool{
		"endpoint": true, "access-key": true, "secret-key": true, "bucket": true, "secure": true,
	}
	for _, name := range extra {
		set[name] = true
	}
	var bad []string
	c.fs.Visit(func(f *flag.Flag) {
		if !set[f.Name] {
			bad = append(bad, f.Name)
		}
	})
	if len(bad) == 0 {
		return true
	}
	sort.Strings(bad)
	reportError(c.stderr, fmt.Errorf("flag %s does not apply to this command", strings.Join(prefixFlagNames(bad), ", ")))
	c.usage()
	return false
}

// prefixFlagNames 给 flag 名加上 "--" 前缀（报错文案用）。
func prefixFlagNames(names []string) []string {
	out := make([]string, len(names))
	for i, name := range names {
		out[i] = "--" + name
	}
	return out
}

// withStore 惰性构造对象存储客户端并执行一次只读动作：构造失败与动作失败都按运行期
// 错误处理（退出码 1）。用法错误在调用方先行返回 2，绝不触碰对象存储。
func (c *cli) withStore(fn func(store objectstore.Store) error) int {
	store, err := c.deps.newStore(c.opts.endpoint, c.opts.accessKey, c.opts.secretKey, c.opts.bucket, c.opts.secure)
	if err != nil {
		reportError(c.stderr, fmt.Errorf("create object store: %w", err))
		return exitRuntimeError
	}
	if err := fn(store); err != nil {
		reportError(c.stderr, err)
		return exitRuntimeError
	}
	return exitOK
}

// lsDatasets 枚举存在 CURRENT 的数据集名：列 dataset/ 前缀，只保留形状恰为
// dataset/<ds>/CURRENT 的键（快照 manifest/分片对象与其他层级一律忽略），
// 去重后按字典序逐行输出。无匹配则不输出任何行。
func (c *cli) lsDatasets(store objectstore.Store) error {
	keys, err := store.List(c.ctx, datasetKeyPrefix)
	if err != nil {
		return fmt.Errorf("list %s: %w", datasetKeyPrefix, err)
	}
	seen := make(map[string]bool, len(keys))
	datasets := make([]string, 0, len(keys))
	for _, key := range keys {
		dataset, ok := datasetNameFromCurrentKey(key)
		if !ok || seen[dataset] {
			continue
		}
		seen[dataset] = true
		datasets = append(datasets, dataset)
	}
	sort.Strings(datasets)
	for _, dataset := range datasets {
		fmt.Fprintln(c.stdout, dataset)
	}
	return nil
}

// datasetNameFromCurrentKey 从对象键解析数据集名：键必须恰为 dataset/<ds>/CURRENT
// 且 <ds> 非空、不含 "/"（否则是快照子对象或多层嵌套的杂项键）。
func datasetNameFromCurrentKey(key string) (string, bool) {
	if !strings.HasPrefix(key, datasetKeyPrefix) {
		return "", false
	}
	rest := strings.TrimSuffix(key[len(datasetKeyPrefix):], currentKeySuffix)
	if rest == key[len(datasetKeyPrefix):] || rest == "" || strings.Contains(rest, "/") {
		return "", false
	}
	return rest, true
}

// lsSnapshots 枚举 dataset/<ds>/snapshots/ 下的快照 ID（只认 manifest.json 键，
// 分片对象不产生条目），按字典序逐行输出；CURRENT 指向的快照追加 " current" 标记。
// CURRENT 不存在时全部按无标记输出（快照对象可以先于 CURRENT 存在，例如提交中断）。
func (c *cli) lsSnapshots(store objectstore.Store, dataset string) error {
	prefix := snapshotsPrefix(dataset)
	keys, err := store.List(c.ctx, prefix)
	if err != nil {
		return fmt.Errorf("list %s: %w", prefix, err)
	}
	snapIDs := snapshotIDs(prefix, keys)

	current, err := readCurrent(c.ctx, store, dataset)
	switch {
	case err == nil:
	case errors.Is(err, objectstore.ErrNotFound):
		current = "" // 无 CURRENT：照常列出快照，不加标记
	default:
		return err
	}
	for _, snapID := range snapIDs {
		if current != "" && snapID == current {
			fmt.Fprintf(c.stdout, "%s current\n", snapID)
			continue
		}
		fmt.Fprintln(c.stdout, snapID)
	}
	return nil
}

// lsEntries 经 index.Catalog.List 枚举条目（D29）：List 自己读 CURRENT、按 D22
// 相关性判定只下载必要的索引分片，所以无需先 Refresh。输出每行
// "<path> <chunk> <size> <fp>"；--limit n（n>0）只保留已排序结果的前 n 条。
func (c *cli) lsEntries(store objectstore.Store, dataset string) error {
	entries, err := index.NewCatalog(store, dataset, index.Config{}).List(c.ctx, c.prefix)
	if err != nil {
		return entryListError(dataset, err)
	}
	if c.limit > 0 && len(entries) > c.limit {
		entries = entries[:c.limit]
	}
	for _, e := range entries {
		fmt.Fprintln(c.stdout, formatEntry(e))
	}
	return nil
}

// statDataset 按 manifest 口径报告当前快照总量：读 CURRENT → GET 并解析该快照的
// manifest.json，分片数 = Shards 长度、条目数 = Σ ShardRef.Entries。全程零分片下载。
// paths/bytes 是条目级总量，manifest 里没有（design.md §6.4：清单只存引用与路径范围，
// 条目在分片里），要取它们必须下载全量索引分片，正是按需加载要避免的开销，所以本形态
// 不报这两项；需要条目级总量用 stat --path（只下相关分片）或 ls entries。
func (c *cli) statDataset(store objectstore.Store, dataset string) error {
	report, manifest, err := c.statHead(store, dataset)
	if err != nil {
		return err
	}
	report.entries = manifestEntries(manifest)
	report.write(c.stdout, false)
	return nil
}

// statPath 报告单个路径（前缀语义，复用 Catalog.List）的过滤后总量：
// entries/paths/bytes 按过滤结果计，shards 仍是当前 manifest 的分片数。
func (c *cli) statPath(store objectstore.Store, dataset string) error {
	report, _, err := c.statHead(store, dataset)
	if err != nil {
		return err
	}
	entries, err := index.NewCatalog(store, dataset, index.Config{}).List(c.ctx, c.path)
	if err != nil {
		return entryListError(dataset, err)
	}
	report.entries = len(entries)
	report.paths, report.bytes = entryTotals(entries)
	report.write(c.stdout, true)
	return nil
}

// statReport 是一次 stat 的输出字段。
type statReport struct {
	dataset string
	snapID  string
	shards  int
	entries int
	paths   int // 仅 withTotals 形态输出
	bytes   int64
}

// write 输出一行 key=value。withTotals 为真时补 paths/bytes 两个条目级字段——
// 它们只有在下载过相关分片后才存在；为假时只给 manifest 就能算出的四项。
func (r statReport) write(w io.Writer, withTotals bool) {
	if withTotals {
		fmt.Fprintf(w, "dataset=%s snapshot=%s shards=%d entries=%d paths=%d bytes=%d\n",
			r.dataset, r.snapID, r.shards, r.entries, r.paths, r.bytes)
		return
	}
	fmt.Fprintf(w, "dataset=%s snapshot=%s shards=%d entries=%d\n",
		r.dataset, r.snapID, r.shards, r.entries)
}

// statHead 读 CURRENT 与当前快照的 manifest，返回已填好数据集名、快照 ID 与分片数
// 的报告骨架及 manifest 本身；两个形态的 stat 都以它开头。
func (c *cli) statHead(store objectstore.Store, dataset string) (statReport, *index.Manifest, error) {
	snapID, err := readCurrent(c.ctx, store, dataset)
	if err != nil {
		if errors.Is(err, objectstore.ErrNotFound) {
			return statReport{}, nil, noCurrentError(dataset)
		}
		return statReport{}, nil, err
	}
	manifest, err := loadManifest(c.ctx, store, dataset, snapID)
	if err != nil {
		return statReport{}, nil, err
	}
	return statReport{dataset: dataset, snapID: snapID, shards: len(manifest.Shards)}, manifest, nil
}

// snapshotsPrefix 返回 dataset/<ds>/snapshots/ 目录前缀（index 包的键函数只到
// <snapID>/ 一层，目录级枚举要自己拼）。
func snapshotsPrefix(dataset string) string {
	return datasetKeyPrefix + dataset + "/" + snapshotsDirName + "/"
}

// snapshotIDs 从 dataset/<ds>/snapshots/ 前缀下的对象键里挑出 manifest.json，
// 解析出快照 ID 并按字典序去重排序；分片对象（shard-<fp8>.idx）不产生条目。
func snapshotIDs(prefix string, keys []string) []string {
	seen := make(map[string]bool, len(keys))
	snapIDs := make([]string, 0, len(keys))
	for _, key := range keys {
		rest := strings.TrimPrefix(key, prefix)
		cut := strings.Index(rest, "/")
		if cut <= 0 || rest[cut+1:] != manifestFileName {
			continue
		}
		snapID := rest[:cut]
		if seen[snapID] {
			continue
		}
		seen[snapID] = true
		snapIDs = append(snapIDs, snapID)
	}
	sort.Strings(snapIDs)
	return snapIDs
}

// readCurrent 读取并解析 dataset/<ds>/CURRENT（内容 "<snapID> <manifestFP8>"，D8），
// 返回当前快照 ID。CURRENT 里的 manifestFP8 本 CLI 不用：指纹校验链由 index.Catalog
// 在下载分片时走完整（stat 的条目级总量正是要经它取）。
// 对象不存在时错误里保留 objectstore.ErrNotFound（errors.Is 可判），调用方据此区分
// "无 CURRENT"与真错误；段数不符即 CURRENT 损坏，返回错误。
func readCurrent(ctx context.Context, store objectstore.Store, dataset string) (snapID string, err error) {
	raw, err := getObject(ctx, store, index.CurrentKey(dataset))
	if err != nil {
		return "", err
	}
	fields := strings.Fields(string(raw))
	if len(fields) != 2 {
		return "", fmt.Errorf("malformed CURRENT for dataset %s: %q (want \"<snapID> <manifestFP8>\")", dataset, raw)
	}
	return fields[0], nil
}

// loadManifest 读取并解析当前快照的 manifest.json（只 GET 这一次，不下载任何分片）。
// snapshot_id 与 CURRENT 记录不符视为 manifest 损坏。
func loadManifest(ctx context.Context, store objectstore.Store, dataset, snapID string) (*index.Manifest, error) {
	key := index.ManifestKey(dataset, snapID)
	raw, err := getObject(ctx, store, key)
	if err != nil {
		if errors.Is(err, objectstore.ErrNotFound) {
			// CURRENT 已提交但 manifest 不在（例如 GC 窗口边界或中断的提交）：算运行期错误。
			return nil, fmt.Errorf("dataset %s: CURRENT points at snapshot %s whose manifest is missing: %w", dataset, snapID, err)
		}
		return nil, err
	}
	var manifest index.Manifest
	if err := json.Unmarshal(raw, &manifest); err != nil {
		return nil, fmt.Errorf("malformed manifest %s: %w", key, err)
	}
	if manifest.SnapshotID != snapID {
		return nil, fmt.Errorf("malformed manifest %s: snapshot_id %q does not match CURRENT %q", key, manifest.SnapshotID, snapID)
	}
	return &manifest, nil
}

// manifestEntries 汇总 manifest 中各分片的条目数之和（ShardRef.Entries）。
func manifestEntries(manifest *index.Manifest) int {
	total := 0
	for _, ref := range manifest.Shards {
		total += int(ref.Entries)
	}
	return total
}

// entryTotals 汇总条目级的字节总量与去重后的路径数。
func entryTotals(entries []index.Entry) (paths int, bytes int64) {
	seen := make(map[string]bool, len(entries))
	for _, e := range entries {
		bytes += e.Size
		if !seen[e.Path] {
			seen[e.Path] = true
			paths++
		}
	}
	return paths, bytes
}

// entryListError 把 Catalog.List 的错误转成 CLI 的运行期错误文案：
// 无 CURRENT 单独说明（多半是数据集还没发布过），其余错误原样包装。
func entryListError(dataset string, err error) error {
	if errors.Is(err, index.ErrNoCurrent) {
		return noCurrentError(dataset)
	}
	return fmt.Errorf("list entries of dataset %s: %w", dataset, err)
}

// noCurrentError 是"该数据集没有 CURRENT"的运行期错误文案。
func noCurrentError(dataset string) error {
	return fmt.Errorf("dataset %s has no CURRENT (no index snapshot committed yet)", dataset)
}

// getObject 读取对象全部内容，错误带上对象键（与 index 包同风格）。
// objectstore.ErrNotFound 经 %w 保留，调用方仍可用 errors.Is 判定"对象不存在"。
func getObject(ctx context.Context, store objectstore.Store, key string) ([]byte, error) {
	var buf bytes.Buffer
	if err := store.Get(ctx, key, &buf); err != nil {
		return nil, fmt.Errorf("get %s: %w", key, err)
	}
	return buf.Bytes(), nil
}

// formatEntry 渲染一行条目："<path> <chunk> <size> <fp>"。
func formatEntry(e index.Entry) string {
	return fmt.Sprintf("%s %d %d %s", e.Path, e.Chunk, e.Size, e.Fingerprint.String())
}

// usage 把命令面与 flag 默认值写到 stderr。
func (c *cli) usage() {
	fmt.Fprintf(c.stderr, "usage: %s <command> [options]\n", programName)
	fmt.Fprintln(c.stderr, "  traincacheds ls datasets")
	fmt.Fprintln(c.stderr, "  traincacheds ls snapshots <ds>")
	fmt.Fprintln(c.stderr, "  traincacheds ls entries   <ds> [--prefix p/] [--limit n]")
	fmt.Fprintln(c.stderr, "  traincacheds stat         <ds> [--path p.tar]")
	if c.fs != nil {
		c.fs.PrintDefaults()
	}
}

// reportError 把运行期错误写到 stderr（带程序名前缀）。
func reportError(stderr io.Writer, err error) {
	fmt.Fprintf(stderr, "%s: %v\n", programName, err)
}

// envOr 返回环境变量值；未设置或全空白时返回默认值（与 trainpublish 同口径）。
func envOr(key, def string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return def
}

// envBool 解析布尔环境变量："1"/"true"/"yes"（大小写不敏感）为真，
// 其余取值及未设置一律为假（与 trainpublish.envBool 同口径）。
func envBool(key string) bool {
	switch strings.ToLower(strings.TrimSpace(os.Getenv(key))) {
	case "1", "true", "yes":
		return true
	}
	return false
}
