package cas

// 引用计数与读者 pin（design.md §5.2、§7.3；decisions.md D9：内存表，不持久化）。
//
//   - 引用计数 = 已提交快照对该 CAS 块的逻辑引用（Acquire/Release）；
//   - 读操作用 Pin/Unpin 单独管理生命周期，不占引用计数。
//
// 两者都只维护进程内计数，不触碰磁盘；Remove 在 Store.mu 内完成
// "检查 refs/pins → 删除文件"，因此判定与删除不会被并发的 Acquire/Pin 插入。

// Acquire 引用计数 +1（引用计数 = 已提交快照对该 CAS 块的逻辑引用）。
func (s *Store) Acquire(fp Fingerprint) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.refs[fp]++
	return nil
}

// Release 引用计数 -1，归零后块进入可回收状态。
// 未 Acquire 过（或已归零）时为空操作，计数不会变为负数。
func (s *Store) Release(fp Fingerprint) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if n := s.refs[fp]; n > 1 {
		s.refs[fp] = n - 1
	} else {
		delete(s.refs, fp)
	}
}

// Pin 读操作生命周期：Pin 计数 >0 时 Remove 必须返回 ErrInUse。
func (s *Store) Pin(fp Fingerprint) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.pins[fp]++
	return nil
}

// Unpin 读操作结束：Pin 计数 -1。
// 未 Pin 过（或已归零）时为空操作，计数不会变为负数。
func (s *Store) Unpin(fp Fingerprint) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if n := s.pins[fp]; n > 1 {
		s.pins[fp] = n - 1
	} else {
		delete(s.pins, fp)
	}
}
