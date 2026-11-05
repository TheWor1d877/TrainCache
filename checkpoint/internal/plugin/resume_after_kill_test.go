package plugin

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

// 本文件补 10.3 §6「续传跨进程」的缺口：续传状态必须能跨进程存活。
// 子进程写完已确认分片后被 SIGKILL，下一次运行读到的仍是那些已确认分片，
// 并从最后一个已确认分片之后继续写，不重写已确认的行（design 10.1 §5.2、
// 10.2 D16）。这里锁的是 plugin.AppendPart 的 fsync 与 LoadParts 的
// 末行丢弃语义，跨进程这一层由真实子进程（不是 goroutine）承担。

const resumeStateEnv = "TC_RESUME_STATE_DIR"

// TestResumeStateChildHelper 是子进程入口：写两条已确认分片记录后挂住，
// 等父进程 SIGKILL。父进程只通过环境变量与文件系统和它交互。
func TestResumeStateChildHelper(t *testing.T) {
	dir := os.Getenv(resumeStateEnv)
	if dir == "" {
		t.Skip("只在子进程里跑")
	}
	path := filepath.Join(dir, "model.bin.jsonl")
	for part := 1; part <= 2; part++ {
		if err := AppendPart(path, PartRecord{Part: part, ETag: fmt.Sprintf("etag-%d", part), Bytes: 8 << 20}); err != nil {
			t.Fatalf("AppendPart(%d) error = %v", part, err)
		}
	}
	// 挂在这里等父进程来杀。SIGKILL 之后不会有任何清理动作，
	// 状态文件里已 fsync 的行必须原样留着。
	for {
		time.Sleep(100 * time.Millisecond)
	}
}

// TestResumeStateSurvivesProcessKill 断言：子进程写完并 fsync 的已确认
// 分片记录，在子进程被 SIGKILL 之后仍然可读；下一次运行从最后一个已确认
// 分片之后继续追加，已确认的分片不被重写。
func TestResumeStateSurvivesProcessKill(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "model.bin.jsonl")

	cmd := exec.Command(os.Args[0], "-test.run=^TestResumeStateChildHelper$", "-test.v")
	cmd.Env = append(os.Environ(), resumeStateEnv+"="+dir)
	var out strings.Builder
	cmd.Stdout = &out
	cmd.Stderr = &out
	if err := cmd.Start(); err != nil {
		t.Fatalf("启动子进程: %v", err)
	}

	// 等子进程把两条记录写完再杀，否则测的是「杀得太早」而不是「杀进程」。
	waitForRecords(t, path, 2)
	if err := cmd.Process.Signal(syscall.SIGKILL); err != nil {
		t.Fatalf("杀子进程: %v", err)
	}
	_ = cmd.Wait()
	if !strings.Contains(out.String(), "TestResumeStateChildHelper") {
		t.Fatalf("子进程没有跑到用例:\n%s", out.String())
	}

	got, err := LoadParts(path)
	if err != nil {
		t.Fatalf("LoadParts() error = %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("子进程被杀后已确认分片数 = %d，期望 2 (%+v)", len(got), got)
	}
	for i, rec := range got {
		want := PartRecord{Part: i + 1, ETag: fmt.Sprintf("etag-%d", i+1), Bytes: 8 << 20}
		if rec != want {
			t.Fatalf("已确认分片[%d] = %+v，期望 %+v", i, rec, want)
		}
	}

	// 第二次运行：接着已确认的分片继续写，不重写前两行。
	for part := 3; part <= 4; part++ {
		if err := AppendPart(path, PartRecord{Part: part, ETag: fmt.Sprintf("etag-%d", part), Bytes: 8 << 20}); err != nil {
			t.Fatalf("第二次运行 AppendPart(%d) error = %v", part, err)
		}
	}
	got, err = LoadParts(path)
	if err != nil {
		t.Fatalf("第二次运行 LoadParts() error = %v", err)
	}
	if len(got) != 4 {
		t.Fatalf("第二次运行后分片记录数 = %d，期望 4 (%+v)", len(got), got)
	}
	for i, rec := range got {
		if rec.Part != i+1 {
			t.Fatalf("分片记录[%d] = %+v，期望 part=%d", i, rec, i+1)
		}
	}
}

// waitForRecords 等状态文件里出现 n 条完整记录（按行数计）。
func waitForRecords(t *testing.T, path string, n int) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		b, err := os.ReadFile(path)
		if err == nil {
			lines := 0
			for _, line := range strings.Split(string(b), "\n") {
				if strings.TrimSpace(line) != "" {
					lines++
				}
			}
			if lines >= n {
				return
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("等待状态文件出现 %d 条记录超时", n)
}
