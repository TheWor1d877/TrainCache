package local

import (
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"traincache/checkpoint/internal/plugin"
)

// 本文件覆盖 10.3 覆盖矩阵里「续传跨进程」在 local 后端上的那一半。local 插件不写
// 续传状态文件（design 10.1「数据格式」：local 插件不写这些文件；10.2 D16 的
// 结论里也写明 local 插件不使用），所以它跨进程要保证的是另一件事：
// 上一次运行的插件进程被 SIGKILL 之后，后端目录里不能留下半个文件，
// 下一次运行仍然能保存出完整内容。

const localResumeEnv = "TC_LOCAL_RESUME_DIR"

// TestLocalResumeChildHelper 是子进程入口：它对一个大文件执行保存，
// 复制进行到一半时被父进程 SIGKILL。
func TestLocalResumeChildHelper(t *testing.T) {
	dir := os.Getenv(localResumeEnv)
	if dir == "" {
		t.Skip("只在子进程里跑")
	}
	raw, err := json.Marshal(map[string]string{"root": filepath.Join(dir, "root")})
	if err != nil {
		t.Fatal(err)
	}
	params := plugin.SaveParams{
		TaskID:    "task",
		Rank:      0,
		WorldSize: 1,
		Policy:    plugin.Policy{Mode: "step", Step: 1},
		Backend:   json.RawMessage(raw),
		Step:      1,
		LocalPath: filepath.Join(dir, "src"),
		StatePath: filepath.Join(dir, "state"),
	}
	if _, err := handle(context.Background(), "save", raw2(params), nil); err != nil {
		t.Fatalf("子进程保存: %v", err)
	}
}

// raw2 把参数编码成协议里的原始 JSON。
func raw2(p plugin.SaveParams) json.RawMessage {
	b, err := json.Marshal(p)
	if err != nil {
		panic(err)
	}
	return json.RawMessage(b)
}

// TestLocalResumeAfterProcessKill 断言：保存进程被 SIGKILL 之后，后端目录
// 里没有半个文件（只有复制用的临时文件），下一次运行仍然保存出与源文件
// 逐字节一致的内容。
func TestLocalResumeAfterProcessKill(t *testing.T) {
	dir := t.TempDir()
	src := filepath.Join(dir, "src")
	if err := os.MkdirAll(src, 0o755); err != nil {
		t.Fatal(err)
	}
	// 复制要慢到能被杀掉：文件越大，中途被杀的窗口越长。
	data := make([]byte, 256<<20)
	for i := range data {
		data[i] = byte(i % 251)
	}
	if err := os.WriteFile(filepath.Join(src, "model.bin"), data, 0o644); err != nil {
		t.Fatal(err)
	}
	root := filepath.Join(dir, "root")

	cmd := exec.Command(os.Args[0], "-test.run=^TestLocalResumeChildHelper$", "-test.v")
	cmd.Env = append(os.Environ(), localResumeEnv+"="+dir)
	var out strings.Builder
	cmd.Stdout = &out
	cmd.Stderr = &out
	if err := cmd.Start(); err != nil {
		t.Fatalf("启动子进程: %v", err)
	}
	// 等复制用的临时文件出现，说明复制已经开始，再杀进程。
	tmp := waitForCopyTemp(t, root)
	if err := cmd.Process.Signal(syscall.SIGKILL); err != nil {
		t.Fatalf("杀子进程: %v", err)
	}
	_ = cmd.Wait()

	// 临时文件还在，说明进程确实死在复制中途。
	if _, err := os.Stat(tmp); err != nil {
		t.Fatalf("复制临时文件 %s 不见了，说明杀得太晚: %v", tmp, err)
	}
	// 目标文件此刻必须不存在：它要么没写，要么是完整内容，不能是半个。
	target := filepath.FromSlash(plugin.FileKey(root, "task", 1, 0, "model.bin"))
	if body, err := os.ReadFile(target); err == nil {
		if len(body) != len(data) {
			t.Fatalf("被杀后目标文件长度 = %d，期望不存在或等于 %d", len(body), len(data))
		}
		t.Fatalf("被杀后目标文件已存在（长度 %d），说明复制已经完成，没测到中途崩溃", len(body))
	} else if !os.IsNotExist(err) {
		t.Fatalf("读目标文件: %v", err)
	}
	// manifest 是提交标记，被杀时还没写出来。
	manifest := filepath.FromSlash(plugin.ManifestKey(root, "task", 1, 0))
	if _, err := os.Stat(manifest); err == nil {
		t.Fatal("被杀之后后端目录里出现了 manifest，说明半成品被当成完成版本")
	}

	// 第二次运行：同样的源目录，保存出完整内容。
	raw, err := json.Marshal(map[string]string{"root": root})
	if err != nil {
		t.Fatal(err)
	}
	params := plugin.SaveParams{
		TaskID:    "task",
		Rank:      0,
		WorldSize: 1,
		Policy:    plugin.Policy{Mode: "step", Step: 1},
		Backend:   json.RawMessage(raw),
		Step:      1,
		LocalPath: src,
		StatePath: filepath.Join(dir, "state"),
	}
	if _, err := handle(context.Background(), "save", raw2(params), nil); err != nil {
		t.Fatalf("第二次保存失败: %v", err)
	}
	body, err := os.ReadFile(target)
	if err != nil {
		t.Fatalf("读保存结果: %v", err)
	}
	if len(body) != len(data) {
		t.Fatalf("保存结果长度 = %d，期望 %d", len(body), len(data))
	}
	for i := range body {
		if body[i] != data[i] {
			t.Fatalf("保存结果第 %d 字节 = %d，期望 %d", i, body[i], data[i])
		}
	}
	if _, err := os.Stat(manifest); err != nil {
		t.Fatalf("第二次保存后没有 manifest: %v", err)
	}
}

// waitForCopyTemp 等后端目录里出现复制用的临时文件，返回它的路径。
// 临时文件名形如 model.bin.tmp-123456，因此按后缀匹配即可。
func waitForCopyTemp(t *testing.T, root string) string {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		if p := findCopyTemp(root); p != "" {
			return p
		}
		time.Sleep(200 * time.Microsecond)
	}
	t.Fatalf("等待复制临时文件出现超时")
	return ""
}

// findCopyTemp 在 root 下找第一个复制用的临时文件。filepath.Glob 不支持
// 递归，这里自己走一遍目录树。
func findCopyTemp(root string) string {
	found := ""
	_ = filepath.WalkDir(root, func(p string, d os.DirEntry, err error) error {
		if err != nil {
			return nil
		}
		if d.IsDir() {
			return nil
		}
		if strings.Contains(d.Name(), ".tmp-") {
			found = p
			return filepath.SkipAll
		}
		return nil
	})
	return found
}
