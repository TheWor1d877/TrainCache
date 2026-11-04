package plugin

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

// buildFakePlugin 把 testdata 下的假插件编到当前测试的临时目录。
// testdata 不参与 ./... 的常规编译，只有需要进程级契约时才现编。
func buildFakePlugin(t *testing.T) string {
	t.Helper()
	bin := filepath.Join(t.TempDir(), "fakeplugin")
	cmd := exec.Command("go", "build", "-o", bin, "./testdata/fakeplugin")
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("go build fakeplugin: %v\n%s", err, out)
	}
	return bin
}

func startFake(t *testing.T, mode string, opts Options) (*Client, string) {
	t.Helper()
	if opts.Path == "" {
		opts.Path = buildFakePlugin(t)
	}
	if opts.SocketPath == "" {
		opts.SocketPath = filepath.Join(t.TempDir(), "plugin.sock")
	}
	if opts.HandshakeTimeout == 0 {
		opts.HandshakeTimeout = 3 * time.Second
	}
	if opts.LivenessTimeout == 0 {
		opts.LivenessTimeout = 3 * time.Second
	}
	t.Setenv("FAKEPLUGIN_MODE", mode)

	c, err := Start(context.Background(), opts)
	if err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c, opts.SocketPath
}

func TestClientResolve(t *testing.T) {
	c, sock := startFake(t, "normal", Options{})
	res, err := c.Resolve(context.Background(), ResolveParams{
		TaskID:    "task-0",
		Rank:      1,
		WorldSize: 8,
		Policy:    Policy{Mode: "latest"},
	})
	if err != nil {
		t.Fatalf("Resolve() error = %v", err)
	}
	want := ResolveResult{Found: true, Step: 7, Complete: true, Epoch: 2}
	if res != want {
		t.Fatalf("Resolve() = %+v, want %+v", res, want)
	}
	if err := c.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if _, err := os.Stat(sock); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("Close() 后 socket 仍存在: %v", err)
	}
}

func TestStartHandshakeTimeoutKillsProcess(t *testing.T) {
	bin := buildFakePlugin(t)
	sock := filepath.Join(t.TempDir(), "plugin.sock")
	pidFile := filepath.Join(t.TempDir(), "plugin.pid")
	t.Setenv("FAKEPLUGIN_MODE", "no_connect")
	t.Setenv("FAKEPLUGIN_PIDFILE", pidFile)

	start := time.Now()
	c, err := Start(context.Background(), Options{
		Path:             bin,
		SocketPath:       sock,
		HandshakeTimeout: 200 * time.Millisecond,
	})
	if err == nil {
		_ = c.Close()
		t.Fatal("Start() error = nil, want handshake timeout")
	}
	if !strings.Contains(err.Error(), "握手超时") {
		t.Fatalf("Start() error = %v, want 握手超时", err)
	}
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Fatalf("握手超时耗时 = %v, want < 5s", elapsed)
	}

	pid := waitPIDFile(t, pidFile)
	waitProcessGone(t, pid)
	if _, err := os.Stat(sock); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("超时后 socket 仍存在: %v", err)
	}
}

func TestClientAbnormalExitIsInternal(t *testing.T) {
	c, _ := startFake(t, "no_result_exit", Options{})
	_, err := c.Resolve(context.Background(), ResolveParams{TaskID: "task-0", WorldSize: 1})
	if err == nil {
		t.Fatal("Resolve() error = nil, want INTERNAL")
	}
	var pluginErr *Error
	if !errors.As(err, &pluginErr) {
		t.Fatalf("Resolve() error = %T %v, want *Error", err, err)
	}
	if pluginErr.Code != CodeInternal {
		t.Fatalf("CodeOf(error) = %s, want INTERNAL", pluginErr.Code)
	}
}

func TestClientProgress(t *testing.T) {
	var mu sync.Mutex
	var got [][2]int64
	c, _ := startFake(t, "progress", Options{
		OnProgress: func(done, total int64) {
			mu.Lock()
			got = append(got, [2]int64{done, total})
			mu.Unlock()
		},
	})

	res, err := c.Save(context.Background(), SaveParams{
		TaskID:    "task-0",
		Rank:      0,
		WorldSize: 1,
		Policy:    Policy{Mode: "step", Step: 7},
		Step:      7,
		LocalPath: "/checkpoints-work/task-0",
		StatePath: "/checkpoints-nvme/task-0/state",
	})
	if err != nil {
		t.Fatalf("Save() error = %v", err)
	}
	if res != (TransferResult{Bytes: 100, Files: 1}) {
		t.Fatalf("Save() = %+v", res)
	}

	mu.Lock()
	progress := append([][2]int64(nil), got...)
	mu.Unlock()
	want := [][2]int64{{40, 100}, {100, 100}}
	if len(progress) != len(want) {
		t.Fatalf("progress = %v, want %v", progress, want)
	}
	for i := range want {
		if progress[i] != want[i] {
			t.Fatalf("progress[%d] = %v, want %v", i, progress[i], want[i])
		}
	}
}

func TestClientCancelOnClose(t *testing.T) {
	progress := make(chan struct{}, 1)
	c, _ := startFake(t, "cancel", Options{
		OnProgress: func(_, _ int64) {
			select {
			case progress <- struct{}{}:
			default:
			}
		},
	})

	errCh := make(chan error, 1)
	go func() {
		_, err := c.Save(context.Background(), SaveParams{TaskID: "task-0", Step: 1, WorldSize: 1})
		errCh <- err
	}()

	select {
	case <-progress:
	case <-time.After(5 * time.Second):
		t.Fatal("没有收到 progress，请求未到达插件")
	}

	start := time.Now()
	if err := c.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Fatalf("Close() 耗时 = %v, want <= 5s", elapsed)
	}

	select {
	case err := <-errCh:
		var pluginErr *Error
		if !errors.As(err, &pluginErr) || pluginErr.Code != CodeInternal {
			t.Fatalf("Save() error = %T %v, want INTERNAL", err, err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Close() 后 Save() 未返回")
	}
}

func TestClientLivenessTimeout(t *testing.T) {
	c, _ := startFake(t, "hang", Options{LivenessTimeout: 200 * time.Millisecond})
	_, err := c.Resolve(context.Background(), ResolveParams{TaskID: "task-0", WorldSize: 1})
	if err == nil {
		t.Fatal("Resolve() error = nil, want INTERNAL")
	}
	var pluginErr *Error
	if !errors.As(err, &pluginErr) {
		t.Fatalf("Resolve() error = %T %v, want *Error", err, err)
	}
	if pluginErr.Code != CodeInternal {
		t.Fatalf("Code = %s, want INTERNAL", pluginErr.Code)
	}
}

func waitPIDFile(t *testing.T, path string) int {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		b, err := os.ReadFile(path)
		if err == nil {
			pid, convErr := strconv.Atoi(strings.TrimSpace(string(b)))
			if convErr == nil && pid > 0 {
				return pid
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("等待 pid 文件 %s 超时", path)
	return 0
}

func waitProcessGone(t *testing.T, pid int) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		err := syscall.Kill(pid, 0)
		if errors.Is(err, syscall.ESRCH) {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("进程 %d 在握手超时后仍存活", pid)
}
