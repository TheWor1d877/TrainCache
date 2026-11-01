package merge

import (
	"bytes"
	"context"
	"errors"
	"testing"
)

// pattern 返回长度 n、第 i 字节为 byte(i) 的序列，便于定位绕回错误。
func pattern(n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(i)
	}
	return b
}

// TestRingWrap 覆盖写入 3C 字节后的绕回读：读最后 C 字节必须与源序列一致。
func TestRingWrap(t *testing.T) {
	const c = 8
	r := ring{buf: make([]byte, c)}
	src := pattern(3 * c)
	for off := 0; off < len(src); off += 3 {
		end := off + 3
		if end > len(src) {
			end = len(src)
		}
		r.write(src[off:end])
	}
	if r.written != int64(3*c) {
		t.Fatalf("written = %d, want %d", r.written, 3*c)
	}
	got := r.copyRange(2*c, c)
	if !bytes.Equal(got, src[2*c:]) {
		t.Fatalf("绕回读 = %v, want %v", got, src[2*c:])
	}
}

// TestRingCrossWrap 覆盖从 0 附近跨绕回点的读与部分读。
func TestRingCrossWrap(t *testing.T) {
	const c = 10
	r := ring{buf: make([]byte, c)}
	src := pattern(3*c + 4) // 34 字节，使末尾不是容量的整数倍
	r.write(src)

	// 绝对区间 [29,33)：off=9，跨绕回点。
	if got := r.copyRange(29, 4); !bytes.Equal(got, src[29:33]) {
		t.Fatalf("跨绕回读 = %v, want %v", got, src[29:33])
	}
	if got := r.copyRange(32, 2); !bytes.Equal(got, src[32:34]) {
		t.Fatalf("末尾部分读 = %v, want %v", got, src[32:34])
	}
	if got := r.copyRange(28, 4); !bytes.Equal(got, src[28:32]) {
		t.Fatalf("绕回点部分读 = %v, want %v", got, src[28:32])
	}
}

// TestRingCopyIsDetached 保证交付切片不与环形缓冲共享底层数组。
func TestRingCopyIsDetached(t *testing.T) {
	const c = 4
	r := ring{buf: make([]byte, c)}
	r.write([]byte("abcd"))
	got := r.copyRange(0, 4)
	r.write([]byte("wxyz")) // 覆盖整个缓冲
	if string(got) != "abcd" {
		t.Fatalf("交付切片被后续写入改写: %q", got)
	}
}

// TestAttachBoundary 覆盖 Attach 的 written<=C 边界（恰好 C 与 C+1）。
func TestAttachBoundary(t *testing.T) {
	const c = 8
	reg := NewRegistry(c)
	e := reg.Create("ds/blocks/a#0", reg.GetBuffer())
	defer e.Finish(nil)

	e.write(make([]byte, c))
	if _, ok := reg.Attach("ds/blocks/a#0"); !ok {
		t.Fatal("written == C 时应可加入")
	}

	e.write([]byte{0})
	if _, ok := reg.Attach("ds/blocks/a#0"); ok {
		t.Fatal("written == C+1 时不应加入")
	}
}

// TestRingStreamFromZeroWrap 覆盖订阅者从 pos=0 连续读满 3C 字节：写绕回与
// 读推进同时发生，交付序列必须与源序列逐字节一致。
func TestRingStreamFromZeroWrap(t *testing.T) {
	const c = 8
	src := pattern(3 * c)
	reg := NewRegistry(c)
	e := reg.Create("k", reg.GetBuffer())
	sub := e.Subscribe()
	defer sub.Close()

	var got []byte
	for off := 0; off < len(src); off += 2 {
		e.write(src[off : off+2])
		data, err := sub.Wait(context.Background())
		if err != nil {
			t.Fatalf("off=%d Wait: %v", off, err)
		}
		got = append(got, data...)
	}
	e.Finish(nil)
	if _, err := sub.Wait(context.Background()); !errors.Is(err, ErrClosed) {
		t.Fatalf("结束后 Wait = %v, want ErrClosed", err)
	}
	if !bytes.Equal(got, src) {
		t.Fatalf("从 0 读 3C 字节不一致: got %d bytes", len(got))
	}
}

// TestSubReadsExactlyWritten 保证只交付 [0, written) 的字节，不读出旧数据。
func TestSubReadsExactlyWritten(t *testing.T) {
	const c = 16
	reg := NewRegistry(c)
	e := reg.Create("k", reg.GetBuffer())
	sub := e.Subscribe()
	defer sub.Close()

	e.write([]byte("hello"))
	got, err := sub.Wait(context.Background())
	if err != nil {
		t.Fatalf("Wait: %v", err)
	}
	if string(got) != "hello" {
		t.Fatalf("data = %q, want hello", got)
	}
	e.Finish(nil)

	if _, err := sub.Wait(context.Background()); !errors.Is(err, ErrClosed) {
		t.Fatalf("结束后 Wait = %v, want ErrClosed", err)
	}
}

// TestPumpErrorMatchesFinish 覆盖 Pump 返回值与订阅者观察到的结束错误一致。
func TestPumpErrorMatchesFinish(t *testing.T) {
	reg := NewRegistry(64)
	e := reg.Create("k", reg.GetBuffer())
	sub := e.Subscribe()
	defer sub.Close()

	boom := errors.New("origin exploded")
	pumpErr := e.Pump(context.Background(), &errAfter{data: []byte("abc"), err: boom})
	if !errors.Is(pumpErr, boom) {
		t.Fatalf("Pump = %v, want %v", pumpErr, boom)
	}

	var got []byte
	for {
		b, err := sub.Wait(context.Background())
		got = append(got, b...)
		if err != nil {
			if !errors.Is(err, boom) {
				t.Fatalf("Wait = %v, want %v", err, boom)
			}
			break
		}
	}
	if string(got) != "abc" {
		t.Fatalf("data = %q, want abc", got)
	}
	if _, ok := reg.Attach("k"); ok {
		t.Fatal("结束后 Attach 应为 false")
	}
}

// errAfter 返回一次数据后返回指定错误。
type errAfter struct {
	data []byte
	err  error
	done bool
}

func (e *errAfter) Read(p []byte) (int, error) {
	if e.done {
		return 0, e.err
	}
	e.done = true
	n := copy(p, e.data)
	return n, nil
}
