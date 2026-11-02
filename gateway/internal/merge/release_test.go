package merge

import (
	"context"
	"testing"
)

// TestNoReleaseWhileSubscriberRemains 覆盖「拉取结束但订阅者未离开不归还」。
func TestNoReleaseWhileSubscriberRemains(t *testing.T) {
	const c = 8
	reg := NewRegistry(c)
	buf := reg.GetBuffer()
	e := reg.Create("k", buf)
	sub := e.Subscribe()

	e.write([]byte("abcd"))
	e.Finish(nil)

	// 订阅者还在：池里不应出现这块缓冲，GetBuffer 必须给出另一块。
	other := reg.GetBuffer()
	if &other[0] == &buf[0] {
		t.Fatal("订阅者未离开时缓冲被提前归还")
	}
	if e.Written() != 4 {
		t.Fatalf("Written = %d, want 4", e.Written())
	}
	_ = other

	sub.Close()

	// 最后一个订阅者离开：现在应能取回同一块。
	got := reg.GetBuffer()
	if &got[0] != &buf[0] {
		t.Fatal("最后一个订阅者离开后未归还原缓冲")
	}

	e2 := reg.Create("k2", got)
	if e2.Written() != 0 {
		t.Fatalf("复用后 Written = %d, want 0", e2.Written())
	}
	sub2 := e2.Subscribe()
	e2.write([]byte("ZZZZ"))
	data, err := sub2.Wait(context.Background())
	if err != nil {
		t.Fatalf("Wait: %v", err)
	}
	if string(data) != "ZZZZ" {
		t.Fatalf("复用缓冲交付了旧对象字节: %q", data)
	}
	sub2.Close()
	e2.Finish(nil)
	_ = other
}

// TestReleaseExactlyOnce 覆盖归还只发生一次：两次 Close 不会把同一块缓冲
// 重复放回池（否则两个条目会共享内存）。
func TestReleaseExactlyOnce(t *testing.T) {
	const c = 4
	reg := NewRegistry(c)
	buf := reg.GetBuffer()
	e := reg.Create("k", buf)
	sub := e.Subscribe()

	e.Finish(nil)
	sub.Close()
	sub.Close()

	a := reg.GetBuffer()
	b := reg.GetBuffer()
	if &a[0] == &b[0] {
		t.Fatal("同一块缓冲被重复归还，池里出现了两个相同引用")
	}
	e2 := reg.Create("a", a)
	e3 := reg.Create("b", b)
	if e2.Written() != 0 || e3.Written() != 0 {
		t.Fatal("新条目 written 应为 0")
	}
	e2.Finish(nil)
	e3.Finish(nil)
}
