package cache

import "time"

// last-touch 表记在进程内存里：dataset → 最后出现时间（design 9.1 §6.8；
// 9.2 D33）。
//
// 读命中与写盘成功都刷新这张表；启动时由 Scan 按 dataset 目录 mtime 填。
// Linux 上目录 mtime 只在条目增删时变，纯读不刷新，所以空闲判断只读这张表，
// 不读目录 mtime。
//
// 表与驻写字节账共用 Store.mu，保证「谁最老」与「占多少」是同一份快照。

// touch 刷新 dataset 的最后出现时间。
func (s *Store) touch(dataset string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.touchLocked(dataset, time.Now())
}

// touchLocked 写表，调用方持有 mu。
func (s *Store) touchLocked(dataset string, at time.Time) {
	st := s.datasets[dataset]
	if st == nil {
		st = &datasetState{}
		s.datasets[dataset] = st
	}
	st.touch = at
}

// leastTouchedLocked 挑最久没出现的 dataset，排除 except（请求中的这个）。
// 并列时取名字字典序小的，保证行为可复现。调用方持有 mu。
func (s *Store) leastTouchedLocked(except string) string {
	var name string
	var best time.Time
	for ds, st := range s.datasets {
		if ds == except {
			continue
		}
		switch {
		case name == "":
		case st.touch.Before(best):
		case st.touch.Equal(best) && ds < name:
		default:
			continue
		}
		name, best = ds, st.touch
	}
	return name
}
