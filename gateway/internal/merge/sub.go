package merge

import "context"

// maxChunk 是单次 Wait 最多交付的字节数。环形缓冲容量 C 通常是 10MiB，
// 若一次交付全部 avail，每个订阅者都会临时持有一份 C 大小的拷贝
// （32 个订阅者稳态内存翻倍）。限制在 256KiB 后，瞬时拷贝上界为
// 订阅者数 × 256KiB。
const maxChunk = 256 << 10

// Sub 是一个订阅者。pos 是相对对象起点的绝对读偏移，只由 Wait 推进，
// 因此同一订阅者不应被多个 goroutine 并发 Wait。
type Sub struct {
	entry *Entry
	pos   int64

	kicked bool
	closed bool
}

// Wait 阻塞到有新字节、条目结束或 ctx 取消，返回 [pos, 新 pos) 的一段拷贝，
// 并推进 pos；单次最多交付 maxChunk 字节。可读区间恒为 [pos, written)
// （M4 不变量 1）。
//
//   - written - pos > C 时返回 ErrKicked，并把订阅者从集合摘除。
//   - 条目结束且已读完时返回 ErrClosed；若拉取协程带错误结束且仍有未读
//     字节则先把字节交付完。
func (s *Sub) Wait(ctx context.Context) ([]byte, error) {
	e := s.entry

	for {
		e.mu.Lock()
		if s.kicked {
			e.mu.Unlock()
			return nil, ErrKicked
		}
		if s.closed || e.ring.buf == nil {
			e.mu.Unlock()
			return nil, ErrClosed
		}

		avail := e.ring.written - s.pos
		if avail > int64(e.ring.capacity()) {
			// 落后超过一个缓冲容量：摘除自己，不影响其他订阅者。
			s.kicked = true
			e.removeSubLocked(s)
			buf, fn := e.releaseIfReadyLocked()
			e.broadcastLocked()
			e.mu.Unlock()
			e.doRelease(buf, fn)
			return nil, ErrKicked
		}
		if avail > 0 {
			// 单次交付不超过 maxChunk；pos 只推进实际交付的字节数，
			// 踢出判据仍按 written-pos > C 判断。
			n := avail
			if n > maxChunk {
				n = maxChunk
			}
			data := e.ring.copyRange(s.pos, int(n))
			s.pos += n
			e.mu.Unlock()
			return data, nil
		}
		if e.done {
			err := e.err
			e.mu.Unlock()
			if err != nil {
				return nil, err
			}
			return nil, ErrClosed
		}

		changed := e.changed
		e.mu.Unlock()

		select {
		case <-changed:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

// Close 幂等地移除订阅者。若它是最后一个且拉取协程仍在跑，取消源站请求；
// 若条目已结束，则由最后离开的一方归还缓冲与准入槽（M4 不变量 4）。
func (s *Sub) Close() {
	e := s.entry

	e.mu.Lock()
	if s.closed {
		e.mu.Unlock()
		return
	}
	s.closed = true
	e.removeSubLocked(s)
	e.broadcastLocked()
	buf, fn := e.releaseIfReadyLocked()
	e.mu.Unlock()

	e.doRelease(buf, fn)
}

// Pos 返回订阅者当前的绝对读偏移，供踢出日志使用（design 10.1 §7.2）。
func (s *Sub) Pos() int64 {
	e := s.entry
	e.mu.Lock()
	defer e.mu.Unlock()
	return s.pos
}
