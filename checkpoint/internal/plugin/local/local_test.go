package local

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"traincache/checkpoint/internal/manifest"
	"traincache/checkpoint/internal/plugin"
)

const (
	testTask  = "job-0"
	testWorld = 2
)

// call 直接调用 handler，不经过子进程与 socket：插件逻辑与协议无关，
// sess 传 nil 表示不校验进度上报。
func call(t *testing.T, method string, params any) (any, error) {
	t.Helper()
	raw, err := json.Marshal(params)
	if err != nil {
		t.Fatalf("编码 params: %v", err)
	}
	return handle(context.Background(), method, raw, nil)
}

func rawBackend(t *testing.T, root string) json.RawMessage {
	t.Helper()
	b, err := json.Marshal(map[string]string{"root": root})
	if err != nil {
		t.Fatalf("编码 backend: %v", err)
	}
	return b
}

func resolveParams(t *testing.T, root, task string, rank int, mode string, step int64) plugin.ResolveParams {
	return plugin.ResolveParams{
		TaskID:    task,
		Rank:      rank,
		WorldSize: testWorld,
		Policy:    plugin.Policy{Mode: mode, Step: step},
		Backend:   rawBackend(t, root),
	}
}

func saveParams(t *testing.T, root, task string, step int64, rank int, localPath string) plugin.SaveParams {
	return plugin.SaveParams{
		TaskID:    task,
		Rank:      rank,
		WorldSize: testWorld,
		Policy:    plugin.Policy{Mode: "step", Step: step},
		Backend:   rawBackend(t, root),
		Step:      step,
		LocalPath: localPath,
		StatePath: filepath.Join(t.TempDir(), "state"),
	}
}

func restoreParams(t *testing.T, root, task string, step int64, rank int, target string) plugin.RestoreParams {
	return plugin.RestoreParams{
		TaskID:     task,
		Rank:       rank,
		WorldSize:  testWorld,
		Policy:     plugin.Policy{Mode: "step", Step: step},
		Backend:    rawBackend(t, root),
		Step:       step,
		TargetPath: target,
		StatePath:  filepath.Join(t.TempDir(), "state"),
	}
}

// writeFiles 在 dir 下按相对路径建文件，父目录自动创建。
func writeFiles(t *testing.T, dir string, files map[string]string) {
	t.Helper()
	for name, content := range files {
		path := filepath.Join(dir, filepath.FromSlash(name))
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatalf("建目录 %s: %v", path, err)
		}
		if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
			t.Fatalf("写文件 %s: %v", path, err)
		}
	}
}

func mustCode(t *testing.T, err error, want plugin.Code) {
	t.Helper()
	if err == nil {
		t.Fatalf("期望错误码 %s，实际成功", want)
	}
	if got := plugin.CodeOf(err); got != want {
		t.Fatalf("错误码 %s，期望 %s（err=%v）", got, want, err)
	}
}

// saveAllRanks 把一个 step 的所有 rank 都保存一遍，构成完整版本。
func saveAllRanks(t *testing.T, root, src string, step int64) {
	t.Helper()
	for rank := 0; rank < testWorld; rank++ {
		if _, err := call(t, "save", saveParams(t, root, testTask, step, rank, src)); err != nil {
			t.Fatalf("保存 rank %d step %d: %v", rank, step, err)
		}
	}
}

func TestSaveResolveRestoreRoundTrip(t *testing.T) {
	root := t.TempDir()
	src := t.TempDir()
	want := map[string]string{
		"model.bin":         strings.Repeat("a", 1024),
		"sub/optimizer.bin": "opt",
		// 两个标记文件属于 checkpointd 与训练程序的约定，不应进后端。
		doneMarker:     "",
		completeMarker: "",
	}
	writeFiles(t, src, want)

	saveAllRanks(t, root, src, 100)

	res, err := call(t, "resolve", resolveParams(t, root, testTask, 0, "latest", 0))
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	got := res.(plugin.ResolveResult)
	if !got.Found || !got.Complete || got.Step != 100 {
		t.Fatalf("resolve 结果 %+v，期望 found+complete step=100", got)
	}

	// 重复保存同一 step 覆盖写，结果必须与首次一致。
	saveAllRanks(t, root, src, 100)

	target := filepath.Join(t.TempDir(), "restore")
	out, err := call(t, "restore", restoreParams(t, root, testTask, 100, 0, target))
	if err != nil {
		t.Fatalf("restore: %v", err)
	}
	tr := out.(plugin.TransferResult)
	if tr.Files != 2 {
		t.Fatalf("恢复文件数 %d，期望 2（不应含标记文件）", tr.Files)
	}
	if tr.Bytes != 1027 {
		t.Fatalf("恢复字节数 %d，期望 1027", tr.Bytes)
	}
	for name, content := range want {
		if name == doneMarker || name == completeMarker {
			if _, err := os.Stat(filepath.Join(target, name)); !errors.Is(err, fs.ErrNotExist) {
				t.Fatalf("标记文件 %s 不应被恢复", name)
			}
			continue
		}
		body, err := os.ReadFile(filepath.Join(target, filepath.FromSlash(name)))
		if err != nil {
			t.Fatalf("读恢复文件 %s: %v", name, err)
		}
		if string(body) != content {
			t.Fatalf("恢复文件 %s 内容不一致: %d 字节", name, len(body))
		}
	}

	// manifest 里的 files 必须按 path 排序，且不含标记文件。
	m, err := readManifest(filepath.FromSlash(plugin.ManifestKey(root, testTask, 100, 0)))
	if err != nil {
		t.Fatalf("读 manifest: %v", err)
	}
	if got, wantPaths := filePaths(m), []string{"model.bin", "sub/optimizer.bin"}; !equalStrings(got, wantPaths) {
		t.Fatalf("manifest files=%v，期望 %v", got, wantPaths)
	}
}

func filePaths(m *manifest.Manifest) []string {
	out := make([]string, 0, len(m.Files))
	for _, f := range m.Files {
		out = append(out, f.Path)
	}
	return out
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func TestResolveSelection(t *testing.T) {
	root := t.TempDir()
	src := t.TempDir()
	writeFiles(t, src, map[string]string{"f.bin": "data"})

	// step 5、6 完整；step 7 只有 rank 0，少一个 rank 不算完整。
	saveAllRanks(t, root, src, 5)
	saveAllRanks(t, root, src, 6)
	if _, err := call(t, "save", saveParams(t, root, testTask, 7, 0, src)); err != nil {
		t.Fatalf("保存 step 7 rank 0: %v", err)
	}

	cases := []struct {
		name      string
		mode      string
		step      int64
		wantFound bool
		wantStep  int64
	}{
		{"latest 取最大完整 step", "latest", 0, true, 6},
		{"mode=step 命中完整 step", "step", 5, true, 5},
		{"mode=step 缺 rank 不完整", "step", 7, false, 0},
		{"mode=step 不存在", "step", 9, false, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			out, err := call(t, "resolve", resolveParams(t, root, testTask, 0, tc.mode, tc.step))
			if err != nil {
				t.Fatalf("resolve: %v", err)
			}
			got := out.(plugin.ResolveResult)
			if got.Found != tc.wantFound || got.Step != tc.wantStep {
				t.Fatalf("resolve=%+v，期望 found=%v step=%d", got, tc.wantFound, tc.wantStep)
			}
			if got.Found != got.Complete {
				t.Fatalf("found=%v 与 complete=%v 应一致：不完整的 step 不会被选中", got.Found, got.Complete)
			}
		})
	}
}

func TestRestoreToExistingTarget(t *testing.T) {
	root := t.TempDir()
	src := t.TempDir()
	writeFiles(t, src, map[string]string{"f.bin": "data"})
	saveAllRanks(t, root, src, 1)

	target := filepath.Join(t.TempDir(), "occupied")
	if err := os.MkdirAll(target, 0o755); err != nil {
		t.Fatalf("建目标目录: %v", err)
	}
	_, err := call(t, "restore", restoreParams(t, root, testTask, 1, 0, target))
	mustCode(t, err, plugin.CodeInvalidArgument)
}

func TestRestoreMissingManifest(t *testing.T) {
	root := t.TempDir()
	_, err := call(t, "restore", restoreParams(t, root, testTask, 42, 0, filepath.Join(t.TempDir(), "out")))
	mustCode(t, err, plugin.CodeNotFound)
}

func TestRestoreCorruptLeavesNoTemp(t *testing.T) {
	root := t.TempDir()
	src := t.TempDir()
	writeFiles(t, src, map[string]string{"model.bin": "payload"})
	saveAllRanks(t, root, src, 3)

	// 长度不变、内容不同：模拟后端落盘出错但文件仍可读。
	backendFile := filepath.FromSlash(plugin.FileKey(root, testTask, 3, 0, "model.bin"))
	if err := os.WriteFile(backendFile, []byte("PAYLOAD"), 0o644); err != nil {
		t.Fatalf("篡改后端文件: %v", err)
	}

	parent := t.TempDir()
	target := filepath.Join(parent, "restore")
	_, err := call(t, "restore", restoreParams(t, root, testTask, 3, 0, target))
	mustCode(t, err, plugin.CodeCorrupt)

	if _, err := os.Stat(target); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("校验失败后目标不应存在: %v", err)
	}
	ents, err := os.ReadDir(parent)
	if err != nil {
		t.Fatalf("列目录: %v", err)
	}
	for _, e := range ents {
		if strings.HasPrefix(e.Name(), filepath.Base(target)+".tmp-") {
			t.Fatalf("残留临时目录 %s", e.Name())
		}
	}
}

func TestSaveRejectsSymlink(t *testing.T) {
	root := t.TempDir()
	src := t.TempDir()
	writeFiles(t, src, map[string]string{"f.bin": "data"})
	link := filepath.Join(src, "link.bin")
	if err := os.Symlink(filepath.Join(src, "f.bin"), link); err != nil {
		t.Fatalf("建软链: %v", err)
	}

	_, err := call(t, "save", saveParams(t, root, testTask, 1, 0, src))
	mustCode(t, err, plugin.CodeInvalidArgument)

	// 拒绝发生在写 manifest 之前，resolve 看不到半成品。
	if _, err := os.Stat(filepath.FromSlash(plugin.ManifestKey(root, testTask, 1, 0))); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("被拒绝的保存不应写 manifest: %v", err)
	}
}

func TestParamValidation(t *testing.T) {
	root := t.TempDir()
	src := t.TempDir()
	writeFiles(t, src, map[string]string{"f.bin": "data"})

	cases := []struct {
		name   string
		method string
		params any
	}{
		{
			"backend.root 为相对路径",
			"resolve",
			plugin.ResolveParams{
				TaskID: testTask, Rank: 0, WorldSize: testWorld,
				Policy: plugin.Policy{Mode: "latest"}, Backend: json.RawMessage(`{"root":"rel"}`),
			},
		},
		{
			"backend.root 为空",
			"resolve",
			plugin.ResolveParams{
				TaskID: testTask, Rank: 0, WorldSize: testWorld,
				Policy: plugin.Policy{Mode: "latest"}, Backend: json.RawMessage(`{"root":""}`),
			},
		},
		{
			"world_size 为 0",
			"resolve",
			plugin.ResolveParams{
				TaskID: testTask, Rank: 0, WorldSize: 0,
				Policy: plugin.Policy{Mode: "latest"}, Backend: rawBackend(t, root),
			},
		},
		{
			"rank 越界",
			"resolve",
			plugin.ResolveParams{
				TaskID: testTask, Rank: 2, WorldSize: testWorld,
				Policy: plugin.Policy{Mode: "latest"}, Backend: rawBackend(t, root),
			},
		},
		{
			"policy 取值未知",
			"resolve",
			plugin.ResolveParams{
				TaskID: testTask, Rank: 0, WorldSize: testWorld,
				Policy: plugin.Policy{Mode: "newest"}, Backend: rawBackend(t, root),
			},
		},
		{
			"task_id 含分隔符",
			"save",
			plugin.SaveParams{
				TaskID: "a/b", Rank: 0, WorldSize: testWorld, Step: 1,
				LocalPath: src, Backend: rawBackend(t, root),
			},
		},
		{
			"step 为负",
			"save",
			plugin.SaveParams{
				TaskID: testTask, Rank: 0, WorldSize: testWorld, Step: -1,
				LocalPath: src, Backend: rawBackend(t, root),
			},
		},
		{
			"local_path 为空",
			"save",
			plugin.SaveParams{
				TaskID: testTask, Rank: 0, WorldSize: testWorld, Step: 1,
				Backend: rawBackend(t, root),
			},
		},
		{
			"target_path 为空",
			"restore",
			plugin.RestoreParams{
				TaskID: testTask, Rank: 0, WorldSize: testWorld, Step: 1,
				Backend: rawBackend(t, root),
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := call(t, tc.method, tc.params)
			mustCode(t, err, plugin.CodeInvalidArgument)
		})
	}

	t.Run("未知方法", func(t *testing.T) {
		_, err := call(t, "copy", plugin.ResolveParams{})
		mustCode(t, err, plugin.CodeInvalidArgument)
	})
}

func TestSaveEmptyDirectory(t *testing.T) {
	root := t.TempDir()
	_, err := call(t, "save", saveParams(t, root, testTask, 1, 0, t.TempDir()))
	mustCode(t, err, plugin.CodeInvalidArgument)
}

func TestMoveReplacesExistingFile(t *testing.T) {
	dir := t.TempDir()
	src := filepath.Join(dir, "src")
	dst := filepath.Join(dir, "dst")
	if err := os.WriteFile(src, []byte("new"), 0o644); err != nil {
		t.Fatalf("写源文件: %v", err)
	}
	if err := os.WriteFile(dst, []byte("old-content"), 0o644); err != nil {
		t.Fatalf("写目标文件: %v", err)
	}
	if err := move(src, dst); err != nil {
		t.Fatalf("move: %v", err)
	}
	body, err := os.ReadFile(dst)
	if err != nil {
		t.Fatalf("读目标文件: %v", err)
	}
	if !bytes.Equal(body, []byte("new")) {
		t.Fatalf("目标内容 %q，期望覆盖为 new", body)
	}
}

// putRankManifest 按后端布局直接写出某个 rank 的 manifest。D25 用例要造的
// 是「编号 ≥ 声明 world_size 的已提交份」，走 save 会被 rank < world_size 的
// 参数校验拦住，只能按布局落对象。
func putRankManifest(t *testing.T, root, task string, step int64, rank, world, epoch int) {
	t.Helper()
	// manifest 自身要求 rank < world_size：多余份来自副本数更大的上一轮，
	// 编号越界时按它的实际编号取最小可用值。
	manifestWorld := world
	if manifestWorld <= rank {
		manifestWorld = rank + 1
	}
	m := &manifest.Manifest{
		Version:   manifest.Version,
		TaskID:    task,
		Step:      step,
		Rank:      rank,
		WorldSize: manifestWorld,
		Epoch:     epoch,
		CreatedAt: time.Now().UTC(),
		Files:     []manifest.File{{Path: "model.bin", Size: 4, SHA256: sha256Hex("data")}},
	}
	data, err := m.Marshal()
	if err != nil {
		t.Fatalf("编码 manifest: %v", err)
	}
	key := filepath.FromSlash(plugin.ManifestKey(root, task, step, rank))
	if err := os.MkdirAll(filepath.Dir(key), 0o755); err != nil {
		t.Fatalf("建 rank 目录: %v", err)
	}
	if err := os.WriteFile(key, data, 0o644); err != nil {
		t.Fatalf("写 manifest: %v", err)
	}
}

func sha256Hex(s string) string {
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:])
}

// TestStepCompleteRequiresExactRankSet 锁住 D25：完整性按 rank 集合恰好
// 等于 {0 … world_size-1} 判定，缺份与多出份都判不完整。
func TestStepCompleteRequiresExactRankSet(t *testing.T) {
	const world = 3
	cases := []struct {
		name         string
		ranks        []int
		wantComplete bool
		wantMaxExtra int
	}{
		{"集合 {0,1,2} 完整", []int{0, 1, 2}, true, 0},
		{"集合 {0,1,3} 缺 2 且多出 3", []int{0, 1, 3}, false, 3},
		{"集合 {0,1,2,3} 多出 3", []int{0, 1, 2, 3}, false, 3},
		{"多份取最大编号", []int{0, 1, 2, 3, 5}, false, 5},
		{"只缺份没有多余", []int{0, 1}, false, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			for _, rank := range tc.ranks {
				putRankManifest(t, root, testTask, 7, rank, world, 2)
			}
			epoch, maxExtra, complete, err := stepComplete(root, testTask, 7, world)
			if err != nil {
				t.Fatalf("stepComplete: %v", err)
			}
			if complete != tc.wantComplete {
				t.Fatalf("complete = %v, want %v", complete, tc.wantComplete)
			}
			if maxExtra != tc.wantMaxExtra {
				t.Fatalf("maxExtra = %d, want %d", maxExtra, tc.wantMaxExtra)
			}
			if tc.wantComplete && epoch != 2 {
				t.Fatalf("epoch = %d, want 2", epoch)
			}
		})
	}
}

// TestStepCompleteIgnoresInFlightAndOddNames 锁住 D25 的边界：多余份只在
// manifest 已写出时才算提交；编号越界与不认识的名字都不参与集合比对。
func TestStepCompleteIgnoresInFlightAndOddNames(t *testing.T) {
	const world = 3
	root := t.TempDir()
	for _, rank := range []int{0, 1, 2} {
		putRankManifest(t, root, testTask, 7, rank, world, 1)
	}
	// rank-9 只有文件没有 manifest：保存中断留下的半成品，不算多余份。
	inflight := filepath.FromSlash(plugin.RankPrefix(root, testTask, 7, 9))
	if err := os.MkdirAll(filepath.Join(inflight, "files"), 0o755); err != nil {
		t.Fatalf("建在途目录: %v", err)
	}
	// step 目录顶层的标记文件、临时名与非数字 rank 目录都不该被当成 rank 份。
	for _, name := range []string{".done", ".complete", "manifest.json.tmp-1", "rank-x", "rank-", "rank-99999999999999999999"} {
		if err := os.MkdirAll(filepath.Join(filepath.FromSlash(plugin.StepPrefix(root, testTask, 7)), name), 0o755); err != nil {
			t.Fatalf("建杂项目录 %s: %v", name, err)
		}
	}
	epoch, maxExtra, complete, err := stepComplete(root, testTask, 7, world)
	if err != nil {
		t.Fatalf("stepComplete: %v", err)
	}
	if !complete || maxExtra != 0 || epoch != 1 {
		t.Fatalf("complete=%v maxExtra=%d epoch=%d, want true/0/1", complete, maxExtra, epoch)
	}
}

// TestResolveCarriesMaxExtraRank 验证多余份能经 resolve 回到 checkpointd：
// step 模式判不完整时带上编号，latest 落到更旧的完整 step 时也不能丢。
func TestResolveCarriesMaxExtraRank(t *testing.T) {
	const world = 3
	root := t.TempDir()
	stepParams := func(mode string, step int64) plugin.ResolveParams {
		return plugin.ResolveParams{
			TaskID: testTask, Rank: 0, WorldSize: world,
			Policy:  plugin.Policy{Mode: mode, Step: step},
			Backend: rawBackend(t, root),
		}
	}
	for _, rank := range []int{0, 1, 2} {
		putRankManifest(t, root, testTask, 9, rank, world, 1)
	}
	putRankManifest(t, root, testTask, 10, 3, world, 1)

	out, err := call(t, "resolve", stepParams("step", 10))
	if err != nil {
		t.Fatalf("resolve step 10: %v", err)
	}
	if got := out.(plugin.ResolveResult); got.Found || got.MaxExtraRank != 3 {
		t.Fatalf("step 模式结果 %+v, want found=false max_extra=3", got)
	}

	out, err = call(t, "resolve", stepParams("latest", 0))
	if err != nil {
		t.Fatalf("resolve latest: %v", err)
	}
	if got := out.(plugin.ResolveResult); !got.Found || got.Step != 9 || got.MaxExtraRank != 3 {
		t.Fatalf("latest 结果 %+v, want found step=9 max_extra=3", got)
	}

	// 干净目录上多余份为 0：字段缺省即「没有」，不能凭空报警。
	clean := t.TempDir()
	for _, rank := range []int{0, 1, 2} {
		putRankManifest(t, clean, testTask, 1, rank, world, 1)
	}
	out, err = call(t, "resolve", plugin.ResolveParams{
		TaskID: testTask, Rank: 0, WorldSize: world,
		Policy: plugin.Policy{Mode: "latest"}, Backend: rawBackend(t, clean),
	})
	if err != nil {
		t.Fatalf("resolve 干净目录: %v", err)
	}
	if got := out.(plugin.ResolveResult); !got.Found || got.MaxExtraRank != 0 {
		t.Fatalf("干净目录结果 %+v, want found 且无多余份", got)
	}
}
