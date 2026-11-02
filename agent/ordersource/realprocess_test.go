package ordersource

import (
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

// realprocess_test.go covers the writer-crash recovery case from docs/7.3
// 后续覆盖计划: a real ordersrc.py process writes generation 1, is killed with
// SIGKILL, and a fresh process writes generation 2. The Reader must notice the
// generation change, drop the old order, and deliver the new subscription.

const ordersrcScript = "../ordersrc/ordersrc.py"

func python3(t *testing.T) string {
	t.Helper()
	py, err := exec.LookPath("python3")
	if err != nil {
		t.Skip("python3 not found on PATH; skipping real sidecar process test")
	}
	if _, err := os.Stat(ordersrcScript); err != nil {
		t.Skipf("ordersrc.py not found at %s: %v", ordersrcScript, err)
	}
	return py
}

func writerArgs(dir, uid, stage, chunks string) []string {
	return []string{
		"--pod-uid", uid,
		"--dataset", "b-track",
		"--stage-id", stage,
		"--chunks", chunks,
		"--orders-dir", dir,
	}
}

// TestReaderRecoversFromRealWriterCrash spawns a long-lived process that runs
// the real write path, kills it with SIGKILL, then restarts the script.
func TestReaderRecoversFromRealWriterCrash(t *testing.T) {
	py := python3(t)
	fastPoll(t)

	dir := t.TempDir()
	const uid = "pod-uid"
	ringPath := filepath.Join(dir, uid+".ring")
	r := newTestReader(t, ringPath)

	// 常驻进程:用真实 write_ring 写第一份订单,然后 sleep 等待被 kill。
	program := strings.Join([]string{
		"import importlib.util, sys, time",
		"spec = importlib.util.spec_from_file_location('ordersrc', " + strconv.Quote(ordersrcScript) + ")",
		"mod = importlib.util.module_from_spec(spec)",
		"spec.loader.exec_module(mod)",
		"parser = mod.build_parser()",
		"args = mod.parse_and_validate(parser, sys.argv[1:])",
		"mod.write_ring(args, mod.build_payload(args))",
		"sys.stdout.flush()",
		"time.sleep(600)",
	}, "\n")
	first := exec.Command(py, append([]string{"-c", program},
		writerArgs(dir, uid, "epoch-1", "a-000.tar:0,a-001.tar:1")...)...)
	if err := first.Start(); err != nil {
		t.Fatalf("start first writer: %v", err)
	}
	waited := false
	t.Cleanup(func() {
		if !waited {
			_ = first.Process.Kill()
			_, _ = first.Process.Wait()
		}
	})

	mustEventually(t, "first order visible", func() bool {
		o, ok := r.CurrentOrder()
		return ok && o.StageID == "epoch-1" && len(o.Chunks) == 2 &&
			o.Chunks[0] == ChunkRef{Path: "a-000.tar", Chunk: 0}
	})
	ch := subscribeTest(t, r)
	_, gen1, _, _ := readerState(r)
	if gen1 != 1 {
		t.Fatalf("generation after first write = %d, want 1", gen1)
	}

	// SIGKILL:写端无优雅收尾,ring 保持最后提交状态。
	if err := first.Process.Kill(); err != nil {
		t.Fatalf("kill first writer: %v", err)
	}
	_, _ = first.Process.Wait()
	waited = true

	// 重启:新的真实进程写第二份订单,generation 前进。
	restart := exec.Command(py, append([]string{ordersrcScript},
		writerArgs(dir, uid, "epoch-2", "b-000.tar:0,b-001.tar:7")...)...)
	if out, err := restart.CombinedOutput(); err != nil {
		t.Fatalf("restart writer: %v (%s)", err, out)
	}

	mustEventually(t, "second order visible", func() bool {
		o, ok := r.CurrentOrder()
		return ok && o.StageID == "epoch-2" && len(o.Chunks) == 2 &&
			o.Chunks[0] == ChunkRef{Path: "b-000.tar", Chunk: 0} &&
			o.Chunks[1] == ChunkRef{Path: "b-001.tar", Chunk: 7}
	})
	_, gen2, seq2, missing := readerState(r)
	if gen2 != gen1+1 {
		t.Fatalf("generation after restart = %d, want %d", gen2, gen1+1)
	}
	if seq2 <= 0 {
		t.Fatalf("write_seq after restart = %d, want positive", seq2)
	}
	if missing {
		t.Fatal("reader reports missing after restart")
	}

	select {
	case got := <-ch:
		if got.StageID != "epoch-2" {
			t.Fatalf("subscription order = %q, want epoch-2", got.StageID)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("no subscription delivery after writer restart")
	}
}
