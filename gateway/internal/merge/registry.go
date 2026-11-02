package merge

import "sync"

// Registry 是在途表：key → *Entry（design 10.1 §4.5）。
//
// 锁顺序：Registry.mu → Entry.mu。Registry 只做映射与缓冲池，不持有
// 条目的数据锁。
type Registry struct {
	mu          sync.Mutex
	entries     map[string]*Entry
	bufferBytes int

	// 缓冲池。free 是最近归还的 LIFO 栈，保证「最后一个订阅者离开后
	// 立刻取回同一块」可复现；超过 hotLimit 的部分交给 sync.Pool（GC 可
	// 回收，符合「池不限容量」的语义）。
	freeMu sync.Mutex
	free   [][]byte
	pool   sync.Pool
}

// hotLimit 是 LIFO 栈保留的缓冲块数上限（32 × 10MiB = 320MiB）。
const hotLimit = 32

// NewRegistry 构造在途表。bufferBytes 是单条目环形缓冲容量 C
// （design 10.1 §5.2，默认 10MiB）。非正值按 1 处理，避免除零。
func NewRegistry(bufferBytes int) *Registry {
	if bufferBytes <= 0 {
		bufferBytes = 1
	}
	return &Registry{
		entries:     make(map[string]*Entry),
		bufferBytes: bufferBytes,
	}
}

// Attach 在条目在途、未结束且 written <= C 时返回它。任一条件不满足
// 返回 false，调用方走独立回源（design 10.1 §4.5；10.2 D8）。
func (r *Registry) Attach(key string) (*Entry, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()

	e := r.entries[key]
	if e == nil {
		return nil, false
	}

	e.mu.Lock()
	defer e.mu.Unlock()
	if e.done || e.ring.buf == nil || e.ring.written > int64(e.ring.capacity()) {
		return nil, false
	}
	return e, true
}

// Create 用 buf 新建条目并登记。若同一 key 已存在，返回已存在条目，
// 不覆盖；此时 buf 的所有权已交给 Registry，会被归还到池（M4 不变量 7）。
//
// server 在「Attach 失败后」持自身 createMu 调用本方法，保证同一 key
// 同时只有一个拉取协程。
func (r *Registry) Create(key string, buf []byte) *Entry {
	r.mu.Lock()
	if e := r.entries[key]; e != nil {
		r.mu.Unlock()
		r.PutBuffer(buf)
		return e
	}
	e := newEntry(r, key, buf)
	r.entries[key] = e
	r.mu.Unlock()
	return e
}

// Len 返回在途条目数，供测试与汇总使用。
func (r *Registry) Len() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.entries)
}

// InFlight 报告 key 是否仍有登记中的条目，不判断它是否还能被追上。
// server 用它区分「无条目」与「有条目但 written > C」：后者必须独立回源
// （design 10.1 §6.2；10.2 D8），而不是新建条目。
func (r *Registry) InFlight(key string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.entries[key] != nil
}

// GetBuffer 取一块长度固定为 bufferBytes 的缓冲。
func (r *Registry) GetBuffer() []byte {
	r.freeMu.Lock()
	if n := len(r.free); n > 0 {
		b := r.free[n-1]
		r.free = r.free[:n-1]
		r.freeMu.Unlock()
		return b
	}
	r.freeMu.Unlock()

	if v := r.pool.Get(); v != nil {
		p := v.(*[]byte)
		return (*p)[:r.bufferBytes]
	}
	return make([]byte, r.bufferBytes)
}

// PutBuffer 把缓冲放回池。调用方必须保证此后再没有人写这块内存。
func (r *Registry) PutBuffer(b []byte) {
	if cap(b) < r.bufferBytes {
		return
	}
	b = b[:r.bufferBytes]

	r.freeMu.Lock()
	if len(r.free) < hotLimit {
		r.free = append(r.free, b)
		r.freeMu.Unlock()
		return
	}
	r.freeMu.Unlock()

	r.pool.Put(&b)
}
