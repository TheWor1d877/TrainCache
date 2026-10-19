package collector

import (
	"testing"

	"traincache/core/cas"
)

// TestOpenFilterSkipsCallbackKeepsNoteOpen:filter 为 true 时 open 回调不触发,但 noteOpen
// 的热度/命中率逻辑照常生效;false 与 nil filter 均正常触发回调。
func TestOpenFilterSkipsCallbackKeepsNoteOpen(t *testing.T) {
	env := newTestEnv(t, nil, CollectorConfig{})
	selfFP, trainFP := testFP(0x81), testFP(0x82)

	var got []cas.Fingerprint
	env.c.SetOpenCallback(func(fp cas.Fingerprint) { got = append(got, fp) })
	env.c.SetOpenFilter(func(fp cas.Fingerprint) bool { return fp == selfFP })

	env.c.openFromWatch(selfFP, env.clock.Now())
	if len(got) != 0 {
		t.Fatalf("Agent 自打开触发了 open 回调: %v", got)
	}
	if _, ok := entryOf(env.c, selfFP); !ok {
		t.Error("被过滤的打开未走 noteOpen(热度缺失)")
	}

	env.c.openFromWatch(trainFP, env.clock.Now())
	if len(got) != 1 || got[0] != trainFP {
		t.Fatalf("训练打开回调收到 %v, want [%v]", got, trainFP)
	}

	env.c.SetOpenFilter(nil)
	env.c.openFromWatch(selfFP, env.clock.Now())
	if len(got) != 2 || got[1] != selfFP {
		t.Errorf("nil filter 后回调收到 %v, want 追加 %v", got, selfFP)
	}
}
