package merge

// ring 是单条目的定长环形缓冲（design 10.1 §5.2）。
//
// 约定：written 是相对对象起点的绝对偏移；偏移 i 的字节存放在
// buf[i % len(buf)]。可读区间由调用方用「pos 与 written 之差不超过容量」
// 保证；本类型只负责绕回读写，不做踢出判定。
type ring struct {
	buf     []byte
	written int64
}

// capacity 返回环形缓冲容量 C。
func (r *ring) capacity() int { return len(r.buf) }

// write 把 p 写入缓冲并推进 written，必要时绕回覆盖最旧的字节。
// 调用方必须持有 Entry.mu。
func (r *ring) write(p []byte) {
	c := int64(len(r.buf))
	if c == 0 {
		return
	}
	for len(p) > 0 {
		off := int(r.written % c)
		n := copy(r.buf[off:], p)
		r.written += int64(n)
		p = p[n:]
	}
}

// copyRange 拷贝绝对区间 [pos, pos+n) 到新切片。n 必须为正，且区间必须落在
// [0, written) 内。返回值不与环形缓冲共享底层数组：拉取协程可以在订阅者
// 写 HTTP 响应的同时继续覆盖缓冲。
func (r *ring) copyRange(pos int64, n int) []byte {
	out := make([]byte, n)
	c := int64(len(r.buf))
	if c == 0 {
		return out
	}
	off := int(pos % c)
	k := copy(out, r.buf[off:])
	if k < n {
		copy(out[k:], r.buf[:n-k])
	}
	return out
}
