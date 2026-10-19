package cas

// openObserverFunc 是 Open 前置观察者的内部函数类型（atomic.Pointer 的存储载体）。
type openObserverFunc func(Fingerprint)

// SetOpenObserver 安装实例级 Open 前置观察者：每次 Store.Open（命中或缺失）在调用
// os.Open 之前以请求指纹触发一次，语义仅为"本进程即将读打开该块"；随后打开成功与否
// 不影响触发。传 nil 清除观察者。nil 安全。
//
// 观察者不改变 Open 的返回值、错误语义、重试或校验行为；回调调用时不持有 Store 的锁
// （先原子取出函数指针再调用），可安全调用 Mark 等其他组件。SetOpenObserver 与并发
// Open 数据竞争安全。
func (s *Store) SetOpenObserver(fn func(Fingerprint)) {
	if fn == nil {
		s.openObserver.Store(nil)
		return
	}
	h := openObserverFunc(fn)
	s.openObserver.Store(&h)
}

// notifyOpen 取观察者快照并调用（无观察者时 no-op；不持锁）。
func (s *Store) notifyOpen(fp Fingerprint) {
	if h := s.openObserver.Load(); h != nil {
		(*h)(fp)
	}
}
