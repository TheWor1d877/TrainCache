// Package local 实现本地目录后端插件（design 10.1「本地目录契约」、「数据格式」）。
//
// 本地后端把 plugin 包定义的远端键直接映射成 root 下的路径，用于单机验证、
// PVC 与节点本地盘。写入顺序与其它后端一致：先写全部文件对象，最后写
// manifest.json；manifest 是提交标记，因此中途失败只会留下 resolve 看不见
// 的半成品，不会被当成完整 step。
package local

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"

	"traincache/checkpoint/internal/manifest"
	"traincache/checkpoint/internal/plugin"
)

// Name 是握手时声明的插件名，与安装目录 cmd/plugin-local 对应。
// 版本由命令入口的变量提供，构建脚本用 -X 注入。
const Name = "local"

const (
	methodResolve = "resolve"
	methodSave    = "save"
	methodRestore = "restore"

	policyLatest = "latest"
	policyStep   = "step"

	// stepDirPrefix 与 plugin.StepPrefix 生成的目录名同源，只用于枚举时
	// 解析 step 号；所有写入路径仍由 layout 函数生成。
	stepDirPrefix = "step-"

	// 两个标记文件由训练程序与 checkpointd 维护（design 10.1「本地目录契约」），
	// 不属于训练数据，保存时必须排除。
	doneMarker     = ".done"
	completeMarker = ".complete"

	// 卷靠 fsGroup 共享，checkpointd 非 root，复制出来的副本必须对组可读。
	filePerm fs.FileMode = 0o644
	dirPerm  fs.FileMode = 0o755
)

// Handler 返回插件业务入口，交由 plugin.Serve 处理握手与帧编解码。
func Handler() plugin.Handler { return handle }

// handle 按方法分发。参数结构直接复用 plugin 包里的 Params 类型，
// 保证插件与服务端对字段名只有一份定义。
func handle(ctx context.Context, method string, params json.RawMessage, sess *plugin.Session) (any, error) {
	switch method {
	case methodResolve:
		var p plugin.ResolveParams
		if err := decodeParams(params, &p); err != nil {
			return nil, err
		}
		return resolve(p)
	case methodSave:
		var p plugin.SaveParams
		if err := decodeParams(params, &p); err != nil {
			return nil, err
		}
		return save(ctx, p, sess)
	case methodRestore:
		var p plugin.RestoreParams
		if err := decodeParams(params, &p); err != nil {
			return nil, err
		}
		return restore(ctx, p, sess)
	default:
		return nil, errInvalidArgument("未知方法 %q", method)
	}
}

func decodeParams(raw json.RawMessage, v any) error {
	if len(raw) == 0 {
		return errInvalidArgument("params 不能为空")
	}
	if err := json.Unmarshal(raw, v); err != nil {
		return errInvalidArgument("params 解码: %v", err)
	}
	return nil
}

// backend 是 backend 参数的结构。local 插件只认 root，其余键忽略，
// 方便 checkpointd 把同一份配置透传给不同插件。
type backend struct {
	Root string `json:"root"`
}

func parseBackend(raw json.RawMessage) (backend, error) {
	var b backend
	if err := decodeParams(raw, &b); err != nil {
		return b, err
	}
	if b.Root == "" {
		return b, errInvalidArgument("backend.root 不能为空")
	}
	// root 必须是绝对路径：相对路径会随插件进程的工作目录变化，
	// 同一次保存与恢复可能落到不同位置。
	if !filepath.IsAbs(b.Root) {
		return b, errInvalidArgument("backend.root 必须是绝对路径: %q", b.Root)
	}
	b.Root = filepath.Clean(b.Root)
	return b, nil
}

// scope 把一次调用的参数与后端根目录绑在一起。所有远端路径都经
// plugin 包的 layout 函数生成，插件内不另拼一套键格式。
type scope struct {
	root   string
	taskID string
	step   int64
	rank   int
}

func newScope(root, taskID string, step int64, rank int) scope {
	return scope{root: root, taskID: taskID, step: step, rank: rank}
}

func (s scope) manifestPath() string {
	return filepath.FromSlash(plugin.ManifestKey(s.root, s.taskID, s.step, s.rank))
}

func (s scope) filePath(rel string) string {
	return filepath.FromSlash(plugin.FileKey(s.root, s.taskID, s.step, s.rank, rel))
}

// taskDir 是 <root>/<task_id>。layout 包没有导出 task 前缀，用 step 前缀
// 取父目录得到同一个位置，仍然不重复书写格式。
func (s scope) taskDir() string {
	return filepath.Dir(filepath.FromSlash(plugin.StepPrefix(s.root, s.taskID, 0)))
}

func resolve(p plugin.ResolveParams) (plugin.ResolveResult, error) {
	b, err := parseBackend(p.Backend)
	if err != nil {
		return plugin.ResolveResult{}, err
	}
	if err := validateScope(p.TaskID, p.Rank, p.WorldSize); err != nil {
		return plugin.ResolveResult{}, err
	}
	switch p.Policy.Mode {
	case policyLatest:
	case policyStep:
		if p.Policy.Step < 0 {
			return plugin.ResolveResult{}, errInvalidArgument("policy.step=%d 不能为负", p.Policy.Step)
		}
	default:
		return plugin.ResolveResult{}, errInvalidArgument("policy.mode=%q 需为 latest 或 step", p.Policy.Mode)
	}

	if p.Policy.Mode == policyStep {
		epoch, maxExtra, complete, err := stepComplete(b.Root, p.TaskID, p.Policy.Step, p.WorldSize)
		if err != nil {
			return plugin.ResolveResult{}, errInternal(err)
		}
		if !complete {
			// 指定的 step 不完整时按契约返回未找到，不把半成品交给调用方；
			// 多余 rank 仍要带回去，那是 D25 的 WARN 唯一的素材来源。
			return plugin.ResolveResult{MaxExtraRank: maxExtra}, nil
		}
		return plugin.ResolveResult{Found: true, Step: p.Policy.Step, Complete: true, Epoch: epoch}, nil
	}

	sc := scope{root: b.Root, taskID: p.TaskID}
	steps, err := listSteps(sc.taskDir())
	if err != nil {
		return plugin.ResolveResult{}, errInternal(err)
	}
	// 从大到小找第一个完整 step；不完整的 step 直接跳过，因为契约只认
	// manifest 齐全的 step。
	var maxExtra int
	for i := len(steps) - 1; i >= 0; i-- {
		epoch, extra, complete, err := stepComplete(b.Root, p.TaskID, steps[i], p.WorldSize)
		if err != nil {
			return plugin.ResolveResult{}, errInternal(err)
		}
		if complete {
			// 选中 step 自身没有多余份（有的话上面就判不完整了），带回去的
			// 是更靠新 step 上的编号：异常拓扑只要在本次枚举里出现过就要报。
			return plugin.ResolveResult{Found: true, Step: steps[i], Complete: true, Epoch: epoch, MaxExtraRank: maxExtra}, nil
		}
		if extra > maxExtra {
			maxExtra = extra
		}
	}
	return plugin.ResolveResult{MaxExtraRank: maxExtra}, nil
}

// listSteps 只读 <root>/<task_id>，按数值升序返回 step-N 目录的 N。
// 不认识的名字直接忽略，避免 root 里混入的其它目录影响结果。
func listSteps(taskDir string) ([]int64, error) {
	ents, err := os.ReadDir(taskDir)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, nil
		}
		return nil, err
	}
	steps := make([]int64, 0, len(ents))
	for _, e := range ents {
		if !e.IsDir() {
			continue
		}
		name := e.Name()
		if !strings.HasPrefix(name, stepDirPrefix) {
			continue
		}
		n, err := strconv.ParseInt(strings.TrimPrefix(name, stepDirPrefix), 10, 64)
		if err != nil || n < 0 {
			continue
		}
		steps = append(steps, n)
	}
	sort.Slice(steps, func(i, j int) bool { return steps[i] < steps[j] })
	return steps, nil
}

// stepComplete 按 design 10.2 D25 判完整性：实际存在的 rank 集合必须恰好
// 等于 {0 … worldSize-1}。缺任一份判不完整；出现编号 ≥ worldSize 的已提交
// 份同样判不完整，并把看到过的最大编号经 maxExtra 带回（0 表示没有多余份）。
// epoch 取第一个可解析的 manifest；rank 从 0 开始扫描，因此 rank 0 存在时它优先。
func stepComplete(root, taskID string, step int64, worldSize int) (epoch int, maxExtra int, complete bool, err error) {
	// 先扫多余份：它让 step 注定不完整，省掉后面 worldSize 次读。
	maxExtra, err = extraMaxRank(root, taskID, step, worldSize)
	if err != nil || maxExtra > 0 {
		return 0, maxExtra, false, err
	}
	gotEpoch := false
	for rank := 0; rank < worldSize; rank++ {
		path := filepath.FromSlash(plugin.ManifestKey(root, taskID, step, rank))
		data, err := os.ReadFile(path)
		if err != nil {
			if errors.Is(err, fs.ErrNotExist) {
				return 0, 0, false, nil
			}
			return 0, 0, false, err
		}
		m, err := manifest.Unmarshal(data)
		if err != nil {
			// 存在但不可解析的 manifest 不算完整。resolve 只挑完整 step，
			// 不必区分“缺失”与“损坏”。
			return 0, 0, false, nil
		}
		if m.TaskID != taskID || m.Step != step || m.Rank != rank || m.WorldSize != worldSize {
			// 目录被换过或 manifest 放错位置，不能算完整。
			return 0, 0, false, nil
		}
		if !gotEpoch {
			epoch = m.Epoch
			gotEpoch = true
		}
	}
	return epoch, 0, true, nil
}

// extraMaxRank 枚举 step 目录下编号 ≥ worldSize 的 rank 子目录，只把
// 「manifest 存在、能解析且 task_id/step/rank 与目录相符」的份算作已提交：
// manifest 是提交标记，没有它的目录是在途半成品，不该判不完整。
// 有意不比较它的 world_size——副本数变小正是这里要发现的异常本身，
// 按请求值比对只会把 anomaly 藏起来。
func extraMaxRank(root, taskID string, step int64, worldSize int) (int, error) {
	ents, err := os.ReadDir(filepath.FromSlash(plugin.StepPrefix(root, taskID, step)))
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return 0, nil
		}
		return 0, err
	}
	maxRank := 0
	for _, e := range ents {
		if !e.IsDir() {
			continue
		}
		rank, ok := parseRankDirName(e.Name())
		if !ok || rank < worldSize {
			continue
		}
		data, err := os.ReadFile(filepath.FromSlash(plugin.ManifestKey(root, taskID, step, rank)))
		if err != nil {
			if errors.Is(err, fs.ErrNotExist) {
				continue
			}
			return 0, err
		}
		m, err := manifest.Unmarshal(data)
		if err != nil {
			continue
		}
		if m.TaskID != taskID || m.Step != step || m.Rank != rank {
			continue
		}
		if rank > maxRank {
			maxRank = rank
		}
	}
	return maxRank, nil
}

// parseRankDirName 严格解析 rank 目录名：前缀之后必须全是数字，编号上限与
// S3 侧 parseRankPrefix 同值，保证两个插件对同一个目录名的取舍一致。
// step 目录下的其它名字（临时目录、标记文件、来路不明的目录）都不算 rank 目录。
func parseRankDirName(name string) (int, bool) {
	const rankDirPrefix = "rank-"
	if !strings.HasPrefix(name, rankDirPrefix) {
		return 0, false
	}
	digits := name[len(rankDirPrefix):]
	if digits == "" {
		return 0, false
	}
	for _, c := range digits {
		if c < '0' || c > '9' {
			return 0, false
		}
	}
	n, err := strconv.Atoi(digits)
	if err != nil || n < 0 || n > 1<<30 {
		return 0, false
	}
	return n, true
}

func save(ctx context.Context, p plugin.SaveParams, sess *plugin.Session) (plugin.TransferResult, error) {
	if err := validateScope(p.TaskID, p.Rank, p.WorldSize); err != nil {
		return plugin.TransferResult{}, err
	}
	if p.Step < 0 {
		return plugin.TransferResult{}, errInvalidArgument("step=%d 不能为负", p.Step)
	}
	b, err := parseBackend(p.Backend)
	if err != nil {
		return plugin.TransferResult{}, err
	}
	if p.LocalPath == "" {
		return plugin.TransferResult{}, errInvalidArgument("local_path 不能为空")
	}

	files, total, err := scanSource(p.LocalPath)
	if err != nil {
		return plugin.TransferResult{}, err
	}
	if len(files) == 0 {
		// manifest 要求 files 非空，空目录保存不出可提交的版本。
		return plugin.TransferResult{}, errInvalidArgument("local_path %s 下没有可保存的文件", p.LocalPath)
	}

	sc := newScope(b.Root, p.TaskID, p.Step, p.Rank)
	// 重新保存同一个 step 时先摘掉旧 manifest：它仍指向上一版文件，
	// 文件被覆盖期间会让 resolve 看到内容已经开始变化的“完整版本”。
	if err := os.Remove(sc.manifestPath()); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return plugin.TransferResult{}, errInternal(err)
	}
	m := &manifest.Manifest{
		Version:   manifest.Version,
		TaskID:    p.TaskID,
		Step:      p.Step,
		Rank:      p.Rank,
		WorldSize: p.WorldSize,
		// 协议的 save 参数没有 epoch 字段，本地插件无从得知训练轮次，
		// 只能写 0；resolve 仍按 manifest 读取，与其它插件口径一致。
		Epoch:     0,
		CreatedAt: time.Now().UTC(),
	}

	var done int64
	for _, rel := range files {
		if err := ctx.Err(); err != nil {
			return plugin.TransferResult{}, errCanceled()
		}
		h := sha256.New()
		n, err := copyFile(filepath.Join(p.LocalPath, filepath.FromSlash(rel)), sc.filePath(rel), h)
		if err != nil {
			return plugin.TransferResult{}, errInternal(err)
		}
		m.Files = append(m.Files, manifest.File{
			Path:   rel,
			Size:   n,
			SHA256: hex.EncodeToString(h.Sum(nil)),
		})
		done += n
		progress(sess, done, total)
	}

	// 最后写 manifest：它是提交标记，文件对象全部落盘后才出现。
	// Marshal 内部按 path 排序后编码，因此写入顺序稳定。
	if err := writeManifest(m, sc.manifestPath()); err != nil {
		return plugin.TransferResult{}, errInternal(err)
	}
	return plugin.TransferResult{Bytes: done, Files: len(m.Files)}, nil
}

func restore(ctx context.Context, p plugin.RestoreParams, sess *plugin.Session) (plugin.TransferResult, error) {
	if err := validateScope(p.TaskID, p.Rank, p.WorldSize); err != nil {
		return plugin.TransferResult{}, err
	}
	if p.Step < 0 {
		return plugin.TransferResult{}, errInvalidArgument("step=%d 不能为负", p.Step)
	}
	b, err := parseBackend(p.Backend)
	if err != nil {
		return plugin.TransferResult{}, err
	}
	if p.TargetPath == "" {
		return plugin.TransferResult{}, errInvalidArgument("target_path 不能为空")
	}
	target := filepath.Clean(p.TargetPath)
	if target == string(filepath.Separator) {
		return plugin.TransferResult{}, errInvalidArgument("target_path 不能是根目录")
	}
	// 目标已存在时不能就地覆盖：恢复结果会与训练程序在用的目录混在一起，
	// 失败后无法回滚，因此直接拒绝。
	if _, err := os.Lstat(target); err == nil {
		return plugin.TransferResult{}, errInvalidArgument("target_path %s 已存在", target)
	} else if !errors.Is(err, fs.ErrNotExist) {
		return plugin.TransferResult{}, errInternal(err)
	}

	sc := newScope(b.Root, p.TaskID, p.Step, p.Rank)
	m, err := readManifest(sc.manifestPath())
	if err != nil {
		return plugin.TransferResult{}, err
	}
	// manifest 是后端上唯一的提交标记，内容与请求不符说明这份对象被写错
	// 或被换过，按校验失败处理。
	if m.TaskID != p.TaskID || m.Step != p.Step || m.Rank != p.Rank || m.WorldSize != p.WorldSize {
		return plugin.TransferResult{}, errCorrupt(
			"manifest 与请求不符: task_id=%q step=%d rank=%d world_size=%d",
			m.TaskID, m.Step, m.Rank, m.WorldSize)
	}

	// 临时目录建在 target 的父目录下，保证与 target 同一文件系统，
	// 校验通过后 rename 才是原子的。
	parent := filepath.Dir(target)
	if err := os.MkdirAll(parent, dirPerm); err != nil {
		return plugin.TransferResult{}, errInternal(err)
	}
	tmp, err := os.MkdirTemp(parent, filepath.Base(target)+".tmp-")
	if err != nil {
		return plugin.TransferResult{}, errInternal(err)
	}
	committed := false
	defer func() {
		if !committed {
			// 校验失败或中途出错都清掉临时目录，不把半成品留在工作卷上。
			_ = os.RemoveAll(tmp)
		}
	}()

	var total int64
	for _, f := range m.Files {
		total += f.Size
	}
	var done int64
	for _, f := range m.Files {
		if err := ctx.Err(); err != nil {
			return plugin.TransferResult{}, errCanceled()
		}
		src := sc.filePath(f.Path)
		dst := filepath.Join(tmp, filepath.FromSlash(f.Path))
		n, err := copyFile(src, dst, nil)
		if err != nil {
			if errors.Is(err, fs.ErrNotExist) {
				return plugin.TransferResult{}, errNotFound("对象 %s 不存在", f.Path)
			}
			return plugin.TransferResult{}, errInternal(err)
		}
		done += n
		progress(sess, done, total)
	}

	// 大小与 sha256 都核对过才 rename：工作卷上的训练目录要么不存在，
	// 要么就是与 manifest 完全一致的内容。
	if err := m.VerifyFiles(tmp); err != nil {
		return plugin.TransferResult{}, errCorrupt("校验 %s 失败: %v", target, err)
	}
	if err := move(tmp, target); err != nil {
		return plugin.TransferResult{}, errInternal(err)
	}
	committed = true
	return plugin.TransferResult{Bytes: done, Files: len(m.Files)}, nil
}

func readManifest(path string) (*manifest.Manifest, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, errNotFound("manifest %s 不存在", path)
		}
		return nil, errInternal(err)
	}
	m, err := manifest.Unmarshal(data)
	if err != nil {
		return nil, errCorrupt("manifest %s 无效: %v", path, err)
	}
	return m, nil
}

// writeManifest 与文件对象一样先写同目录临时名，fsync 后 rename。
func writeManifest(m *manifest.Manifest, dst string) error {
	b, err := m.Marshal()
	if err != nil {
		return err
	}
	dir := filepath.Dir(dst)
	if err := os.MkdirAll(dir, dirPerm); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(dir, filepath.Base(dst)+".tmp-")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	// 失败路径统一删临时文件；rename 成功后清空名字，避免删掉成品。
	defer func() {
		if tmpName != "" {
			_ = os.Remove(tmpName)
		}
	}()
	if _, err := tmp.Write(b); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Chmod(filePerm); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := move(tmpName, dst); err != nil {
		return err
	}
	tmpName = ""
	return nil
}

// scanSource 返回 local_path 下要保存的文件（相对路径，斜杠分隔，已排序）
// 与字节总数。软链与非常规文件一律拒绝：复制软链等于把目标内容默默带进
// 后端，块设备与 FIFO 则可能永远读不完。
func scanSource(root string) ([]string, int64, error) {
	info, err := os.Lstat(root)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, 0, errInvalidArgument("local_path %s 不存在", root)
		}
		return nil, 0, errInternal(err)
	}
	if info.Mode()&fs.ModeSymlink != 0 {
		return nil, 0, errInvalidArgument("local_path %s 是符号链接", root)
	}
	if !info.IsDir() {
		return nil, 0, errInvalidArgument("local_path %s 不是目录", root)
	}

	var (
		rels  []string
		total int64
	)
	err = filepath.WalkDir(root, func(p string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if p == root {
			return nil
		}
		name := d.Name()
		// 标记文件只出现在 step 目录顶层，这里按名字整体排除，
		// 避免任何层级上的同名文件被当成训练数据上传。
		if name == doneMarker || name == completeMarker {
			if d.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}
		if d.Type()&fs.ModeSymlink != 0 {
			return errInvalidArgument("本地路径 %s 是符号链接", p)
		}
		if d.IsDir() {
			return nil
		}
		if !d.Type().IsRegular() {
			return errInvalidArgument("本地路径 %s 不是常规文件", p)
		}
		fi, err := d.Info()
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(root, p)
		if err != nil {
			return err
		}
		rels = append(rels, filepath.ToSlash(rel))
		total += fi.Size()
		return nil
	})
	if err != nil {
		// 回调里的 *plugin.Error 会被 WalkDir 原样返回，保留错误码；
		// 其它 I/O 错误按 INTERNAL 处理。
		var pe *plugin.Error
		if errors.As(err, &pe) {
			return nil, 0, err
		}
		return nil, 0, errInternal(err)
	}
	sort.Strings(rels)
	return rels, total, nil
}

// copyFile 把 src 的完整内容写到 dst：先在 dst 同目录建临时名，边复制边
// 落盘，fsync 后再 rename。dst 因此要么不存在，要么是完整文件。h 为 nil
// 时不计算摘要，用于恢复路径（校验由 manifest.VerifyFiles 统一做）。
func copyFile(src, dst string, h hash.Hash) (int64, error) {
	dir := filepath.Dir(dst)
	if err := os.MkdirAll(dir, dirPerm); err != nil {
		return 0, err
	}
	in, err := os.Open(src)
	if err != nil {
		return 0, err
	}
	defer in.Close()

	tmp, err := os.CreateTemp(dir, filepath.Base(dst)+".tmp-")
	if err != nil {
		return 0, err
	}
	tmpName := tmp.Name()
	defer func() {
		if tmpName != "" {
			_ = os.Remove(tmpName)
		}
	}()

	var w io.Writer = tmp
	if h != nil {
		w = io.MultiWriter(tmp, h)
	}
	n, err := io.Copy(w, in)
	if err != nil {
		_ = tmp.Close()
		return 0, err
	}
	// CreateTemp 建的是 0600，卷上还要给同组进程读。
	if err := tmp.Chmod(filePerm); err != nil {
		_ = tmp.Close()
		return 0, err
	}
	// manifest 是提交标记，文件内容必须先落盘，否则崩溃后 manifest
	// 可能指向还没写进磁盘的数据。
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return 0, err
	}
	if err := tmp.Close(); err != nil {
		return 0, err
	}
	if err := move(tmpName, dst); err != nil {
		return 0, err
	}
	tmpName = ""
	return n, nil
}

// move 把 src 放到 dst。同一文件系统直接 rename；EXDEV（源与目标不在同一
// 设备）时退化为“复制 + fsync + rename”，调用方看到的 dst 始终是完整内容。
func move(src, dst string) error {
	err := os.Rename(src, dst)
	if err == nil {
		return nil
	}
	if !errors.Is(err, syscall.EXDEV) {
		return err
	}
	info, err := os.Stat(src)
	if err != nil {
		return err
	}
	if !info.IsDir() {
		if _, err := copyFile(src, dst, nil); err != nil {
			return err
		}
		return os.Remove(src)
	}
	// 目录先复制到 dst 同目录的临时名，再 rename，避免半个目录可见。
	tmp, err := os.MkdirTemp(filepath.Dir(dst), filepath.Base(dst)+".tmp-")
	if err != nil {
		return err
	}
	if err := copyTree(src, tmp); err != nil {
		_ = os.RemoveAll(tmp)
		return err
	}
	if err := os.Rename(tmp, dst); err != nil {
		_ = os.RemoveAll(tmp)
		return err
	}
	return os.RemoveAll(src)
}

// copyTree 递归复制目录，只服务 move 的 EXDEV 退化路径。源已经过校验，
// 这里遇到软链或特殊文件直接报错，避免把别的内容混进目标。
func copyTree(src, dst string) error {
	return filepath.WalkDir(src, func(p string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		rel, err := filepath.Rel(src, p)
		if err != nil {
			return err
		}
		target := dst
		if rel != "." {
			target = filepath.Join(dst, rel)
		}
		if d.IsDir() {
			return os.MkdirAll(target, dirPerm)
		}
		if !d.Type().IsRegular() {
			return fmt.Errorf("本地后端: %s 不是常规文件", p)
		}
		_, err = copyFile(p, target, nil)
		return err
	})
}

// progress 上报已传字节。sess 为 nil 表示单元测试直接调用业务函数，
// 此时没有会话可写。上报失败不影响结果：对端断开后 result 帧同样发不出去。
func progress(sess *plugin.Session, done, total int64) {
	if sess == nil {
		return
	}
	_ = sess.Progress(done, total)
}

// validateScope 校验公共参数。task_id 会拼进路径，必须挡住 .. 与分隔符。
func validateScope(taskID string, rank, worldSize int) error {
	if taskID == "" {
		return errInvalidArgument("task_id 不能为空")
	}
	if taskID == "." || taskID == ".." || strings.ContainsAny(taskID, "/\\") {
		return errInvalidArgument("task_id %q 不能包含路径分隔符或为 . / ..", taskID)
	}
	if worldSize <= 0 {
		return errInvalidArgument("world_size=%d 需 > 0", worldSize)
	}
	if rank < 0 || rank >= worldSize {
		return errInvalidArgument("rank=%d 需在 [0,%d) 内", rank, worldSize)
	}
	return nil
}

func errInvalidArgument(format string, args ...any) error {
	return &plugin.Error{Code: plugin.CodeInvalidArgument, Message: fmt.Sprintf(format, args...)}
}

func errNotFound(format string, args ...any) error {
	return &plugin.Error{Code: plugin.CodeNotFound, Message: fmt.Sprintf(format, args...)}
}

func errCorrupt(format string, args ...any) error {
	return &plugin.Error{Code: plugin.CodeCorrupt, Message: fmt.Sprintf(format, args...)}
}

// errInternal 收拢本地 I/O 失败。本地磁盘错误既不是网络不可用也不是
// 参数问题，按契约归到 INTERNAL。
func errInternal(err error) error {
	return &plugin.Error{Code: plugin.CodeInternal, Message: err.Error()}
}

// errCanceled 在收到 cancel 或 SIGTERM 时返回。协议里 cancel 后结果帧
// 已无人接收，错误码只用于诊断，按契约的兜底分类记 INTERNAL。
func errCanceled() error {
	return &plugin.Error{Code: plugin.CodeInternal, Message: "操作已取消"}
}
