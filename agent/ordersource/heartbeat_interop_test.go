package ordersource

// heartbeat_interop_test.go:真实写端 agent/ordersrc/ordersrc.py 造的 ring 喂给真实读端。
//
// D57 的 ring 保活心跳踩的正是这条跨进程契约:Python 侧每次调用都按「新写端会话」算代际,
// Go 侧把代际变化判成 sidecar 重启并按设计《订单通道降级》重放订单。两边各自都「没错」,只有接起来
// 才看得见:每 10s 一次心跳 = 每 10s 一次 OnStageSwitch = 预取队列整段重铺。

import (
	"os/exec"
	"path/filepath"
	"testing"
)

// runOrdersrc 用真实写端跑一次写入,与 bench reader 心跳线程里的 subprocess 同形态。
func runOrdersrc(t *testing.T, args ...string) {
	t.Helper()
	out, err := exec.Command(python3(t), append([]string{ordersrcScript}, args...)...).CombinedOutput()
	if err != nil {
		t.Fatalf("ordersrc.py %v: %v\n%s", args, err, out)
	}
}

func TestOrdersrcHeartbeatStaysInGenerationAndNotRedelivered(t *testing.T) {
	dir := t.TempDir()
	const uid = "pod-hb"
	ringPath := filepath.Join(dir, uid+".ring")
	order := writerArgs(dir, uid, "0", "shard-000.tar:0,shard-007.tar:3")

	fastPoll(t)
	r := newTestReader(t, ringPath)
	ch := subscribeTest(t, r)

	runOrdersrc(t, order...)
	first := recvOrder(t, ch)
	if len(first.Chunks) != 2 || first.StageID != "0" {
		t.Fatalf("首单投递异常: %+v", first)
	}
	attached, gen, seq, _ := readerState(r)
	if !attached || gen != 1 || seq != 1 {
		t.Fatalf("首单状态 = (attached %v, gen %d, seq %d), want (true, 1, 1)", attached, gen, seq)
	}

	for i := 2; i <= 4; i++ {
		runOrdersrc(t, append(append([]string{}, order...), "--same-generation")...)
		expectNoEvent(t, ch) // 同代际同载荷:去重生效,不得触发阶段切换
		_, hbGen, hbSeq, _ := readerState(r)
		if hbGen != 1 {
			t.Fatalf("第 %d 次心跳后代际变成 %d, want 仍为 1(代际一变读端就把订单重放)", i, hbGen)
		}
		if hbSeq != int64(i) {
			t.Fatalf("第 %d 次心跳后 write_seq = %d, want %d(write_seq 不推进则 collector 判停滞)", i, hbSeq, i)
		}
	}

	// 反向半段:不带 --same-generation 的同载荷写入 = 一次虚假的 sidecar 重启 → 重放。
	// 这就是旧心跳的行为,去掉写端开关时本用例即失败。
	runOrdersrc(t, order...)
	assertOrderEqual(t, recvOrder(t, ch), first)
}
