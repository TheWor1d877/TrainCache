// Package merge 实现在途表、环形缓冲、订阅者扇出与踢出
// （design 10.1 §4.5/§5.2/§6.1–§6.3）。
//
// 不可变语义：
//   - 容量 C 的环形缓冲，written 是绝对偏移；订阅者可读区间恒为 [pos, written)。
//   - written - pos > C 的订阅者被踢（ErrKicked），其他订阅者不受影响。
//   - 缓冲只在「拉取协程已结束且订阅者计数为 0」时归还池，且只归还一次。
//   - Finish 幂等；Pump 返回的错误与交给 Finish 的错误一致。
//
// 锁顺序固定为 Registry.mu → Entry.mu；任何路径都不反向获取。
package merge

import (
	"context"
	"errors"
	"io"
	"sync"
	"time"
)

// 订阅者可见的两种终止原因。
var (
	// ErrKicked 表示订阅者落后超过一个缓冲容量，已被摘除。
	ErrKicked = errors.New("merge: subscriber fell behind the ring")
	// ErrClosed 表示条目已正常结束，且没有更多可读字节。
	ErrClosed = errors.New("merge: entry finished")
)

// Entry 是一次在途的整对象下载。所有字段由 mu 保护。
type Entry struct {
	reg *Registry
	key string

	mu      sync.Mutex
	ring    ring
	done    bool
	err     error
	subs    map[*Sub]struct{}
	changed chan struct{} // 关闭并替换以广播状态变化

	// 源站元数据，供 200 响应回填 Last-Modified/ETag；只在首次写字节前设置一次。
	metaSet      bool
	etag         string
	lastModified time.Time

	released  bool
	releaseFn func() // 准入槽释放函数；与缓冲一起只调用一次

	cancel        func() // 拉取协程的取消函数，由 SetCancel 注入
	cancelPending bool   // 订阅者在 Pump 启动前就已全部离开
}

// newEntry 由 Registry 在持锁时调用。
func newEntry(reg *Registry, key string, buf []byte) *Entry {
	return &Entry{
		reg:     reg,
		key:     key,
		ring:    ring{buf: buf},
		subs:    make(map[*Sub]struct{}),
		changed: make(chan struct{}),
	}
}

// Subscribe 挂一个从 pos=0 开始的订阅者。调用方应先经 Attach 确认条目
// 在途且 written <= C（design 10.1 §6.1）。
//
// 只适用于刚 Create 的 owner：owner 不可能已结束，因此不检查 done。
// Attach 命中的路径必须改用 TrySubscribe，否则会在「Attach 与订阅之间
// 条目结束」的窗口里拿到一个立即 ErrClosed 的空订阅者。
func (e *Entry) Subscribe() *Sub {
	e.mu.Lock()
	defer e.mu.Unlock()

	s := &Sub{entry: e}
	if e.released {
		// 缓冲已归还，不能再交付任何字节。
		s.closed = true
		return s
	}
	e.subs[s] = struct{}{}
	return s
}

// TrySubscribe 尝试挂一个从 pos=0 开始的订阅者；条目已结束（done）或缓冲
// 已归还（released）时返回 false，且不加入订阅者集合。
//
// Attach 命中后必须用它：两步之间条目可能 Finish 并因订阅者为 0 立即归还
// 缓冲，此时唯一正确的做法是回退到独立回源，而不是交付空响应。
func (e *Entry) TrySubscribe() (*Sub, bool) {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.done || e.released {
		return nil, false
	}
	s := &Sub{entry: e}
	e.subs[s] = struct{}{}
	return s, true
}

// Pump 从 src 读到 EOF 并写入环形缓冲，结束时调用 Finish。
// 返回的错误与传给 Finish 的错误相同（design 10.1 §4.5；M4 不变量 6）。
// 读 IO 在锁外进行，只有写入缓冲与广播状态在锁内。
//
// ctx 由调用方持有并通过 SetCancel 把取消函数交给条目：最后一个订阅者
// 离开时条目取消这个 ctx，底层源站 Read 才会真正被打断。Pump 自己不再
// 派生 ctx，否则取消的是内层子 ctx，挂在外层请求上的读不会返回。
func (e *Entry) Pump(ctx context.Context, src io.Reader) error {
	e.mu.Lock()
	pending := e.cancelPending
	e.mu.Unlock()

	if pending || ctx.Err() != nil {
		err := ctx.Err()
		if err == nil {
			err = context.Canceled
		}
		e.Finish(err)
		return err
	}

	buf := make([]byte, 32*1024)
	var pumpErr error
	for {
		if err := ctx.Err(); err != nil {
			pumpErr = err
			break
		}
		n, err := src.Read(buf)
		if n > 0 {
			e.write(buf[:n])
		}
		if err != nil {
			if errors.Is(err, io.EOF) {
				pumpErr = nil
			} else {
				pumpErr = err
			}
			break
		}
	}

	e.Finish(pumpErr)
	return pumpErr
}

// Finish 记录结束状态、从 Registry 摘除、唤醒全部订阅者；若此时没有
// 订阅者则立即归还缓冲与准入槽。幂等（M4 不变量 5）。
func (e *Entry) Finish(err error) {
	r := e.reg

	// 与 Attach 保持同一锁序：Registry.mu → Entry.mu。持 Registry.mu 时
	// 完成「标记 done + 从表摘除」，消除 Attach 看到 done 条目后 Create
	// 又拿到它的窗口。
	r.mu.Lock()
	e.mu.Lock()
	if e.done {
		e.mu.Unlock()
		r.mu.Unlock()
		return
	}
	e.done = true
	e.err = err
	e.broadcastLocked()
	if r.entries[e.key] == e {
		delete(r.entries, e.key)
	}
	buf, fn := e.releaseIfReadyLocked()
	e.mu.Unlock()
	r.mu.Unlock()

	e.doRelease(buf, fn)
}

// Written 返回已从源站读入的字节总数。
func (e *Entry) Written() int64 {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.ring.written
}

// SetMetadata 记录源站元数据（ETag 与 Last-Modified）。只在首次写字节前
// 调用一次；重复调用忽略，保证所有订阅者与首个 200 响应看到同一份值。
func (e *Entry) SetMetadata(etag string, lastModified time.Time) {
	e.mu.Lock()
	if !e.metaSet {
		e.metaSet = true
		e.etag = etag
		e.lastModified = lastModified
	}
	e.mu.Unlock()
}

// Metadata 返回首次写字节前记录的源站元数据；未设置时为空串与零值。
func (e *Entry) Metadata() (etag string, lastModified time.Time) {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.etag, e.lastModified
}

// Subscribers 返回当前订阅者数量，供 server 层日志使用（design 10.1 §7.2）。
func (e *Entry) Subscribers() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return len(e.subs)
}

// SetRelease 把准入槽的释放函数交给条目：条目在「拉取协程结束且订阅者
// 清零」时调用它，只调用一次（design 10.1 §6.1 第 7 步）。
//
// 该方法是 server 注入依赖所需的最小扩展；若条目已经释放，立即调用 fn。
func (e *Entry) SetRelease(fn func()) {
	e.mu.Lock()
	if e.released {
		e.mu.Unlock()
		if fn != nil {
			fn()
		}
		return
	}
	e.releaseFn = fn
	e.mu.Unlock()
}

// SetCancel 登记拉取协程的取消函数。幂等；若最后一个订阅者已在登记之前
// 离开（cancelPending），立即调用 cancel，让随后启动的源站请求直接结束。
func (e *Entry) SetCancel(cancel func()) {
	e.mu.Lock()
	e.cancel = cancel
	pending := e.cancelPending
	e.mu.Unlock()

	if pending && cancel != nil {
		cancel()
	}
}

// write 把字节写入环形缓冲并唤醒订阅者。必须在锁外做 IO，在锁内只搬运字节。
func (e *Entry) write(p []byte) {
	e.mu.Lock()
	if e.done || e.ring.buf == nil {
		e.mu.Unlock()
		return
	}
	e.ring.write(p)
	e.broadcastLocked()
	e.mu.Unlock()
}

// broadcastLocked 唤醒所有等待者：关闭当前 channel 并换一个新的。
func (e *Entry) broadcastLocked() {
	close(e.changed)
	e.changed = make(chan struct{})
}

// removeSubLocked 从集合移除订阅者；若这是最后一个且拉取协程仍在跑，
// 取消源站请求（10.2 D10）。
func (e *Entry) removeSubLocked(s *Sub) {
	if _, ok := e.subs[s]; !ok {
		return
	}
	delete(e.subs, s)
	if len(e.subs) == 0 && !e.done {
		if e.cancel != nil {
			e.cancel()
		} else {
			// Pump 还没启动；它启动时会看到该标志并立即结束。
			e.cancelPending = true
		}
	}
}

// releaseIfReadyLocked 在「done 且订阅者为 0 且尚未归还」时标记归还，
// 并交出缓冲与准入释放函数。调用方必须在解锁后执行 doRelease。
func (e *Entry) releaseIfReadyLocked() (buf []byte, fn func()) {
	if !e.done || len(e.subs) > 0 || e.released {
		return nil, nil
	}
	e.released = true
	buf = e.ring.buf
	fn = e.releaseFn
	e.ring.buf = nil
	return buf, fn
}

// doRelease 在锁外归还缓冲、释放准入槽；两者都只发生一次。
func (e *Entry) doRelease(buf []byte, fn func()) {
	if buf != nil {
		e.reg.PutBuffer(buf)
	}
	if fn != nil {
		fn()
	}
}
