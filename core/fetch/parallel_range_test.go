package fetch

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"traincache/core/objectstore"
)

// rangeCall 记录一次 GetRange 的请求区间。
type rangeCall struct {
	start, end int64
}

// testBlockBytes 是并行用例的块大小：拆成 rangeParts 片后每片 16 MiB。
const testBlockBytes int64 = 64 << 20

// orderedRangeStore 模拟慢速分片：各分片以不同速度写入，完成顺序与区间顺序
// 相反；同时统计并发 GetRange 峰值。若实现把分片按完成顺序拼接，测试会得到
// 与整对象不一致的内容。
type orderedRangeStore struct {
	*objectstore.Mock
	data  []byte
	chunk int

	mu    sync.Mutex
	calls []rangeCall
	cur   atomic.Int64
	peak  atomic.Int64
}

func (s *orderedRangeStore) GetRange(ctx context.Context, key string, start, end int64, w io.Writer) error {
	n := s.cur.Add(1)
	defer s.cur.Add(-1)
	updateMax(&s.peak, n)

	s.mu.Lock()
	s.calls = append(s.calls, rangeCall{start: start, end: end})
	s.mu.Unlock()

	// 区间越靠前越慢：完成顺序为 3、2、1、0。
	partLen := end - start + 1
	index := int(start / partLen)
	delay := time.Duration(rangeParts-index) * time.Millisecond
	for off := start; off <= end; off += int64(s.chunk) {
		stop := off + int64(s.chunk)
		if stop > end+1 {
			stop = end + 1
		}
		if _, err := w.Write(s.data[off:stop]); err != nil {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(delay):
		}
	}
	return nil
}

// rangedCalls 返回已记录的 GetRange 区间副本。
func (s *orderedRangeStore) rangedCalls() []rangeCall {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]rangeCall(nil), s.calls...)
}

// TestFetchParallelRangesMatchWholeObject 覆盖并行取数路径：4 个连续区间
// 并行取、完成顺序与区间顺序相反，最终块内容与整对象逐字节一致。
func TestFetchParallelRangesMatchWholeObject(t *testing.T) {
	pattern := make([]byte, 1<<20)
	for i := range pattern {
		pattern[i] = byte(i*31 + 7)
	}
	content := bytes.Repeat(pattern, int(testBlockBytes/int64(len(pattern))))

	store := &orderedRangeStore{Mock: objectstore.NewMock(), data: content, chunk: 1 << 20}
	cs, casDir := newTestCAS(t)

	oldTempDir := rangeTempDir
	rangeTempDir = t.TempDir()
	t.Cleanup(func() { rangeTempDir = oldTempDir })

	f := New(store, cs, Options{RangeParallel: true})
	key := "ds/blocks/big#0"
	fp, err := f.Fetch(context.Background(), key, int64(len(content)))
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	if want := fpOf(content); fp != want {
		t.Fatalf("fp = %s, want %s", fp, want)
	}
	if got := readBlock(t, cs, fp); !bytes.Equal(got, content) {
		t.Fatalf("块内容与整对象不一致：len=%d, want %d", len(got), len(content))
	}

	calls := store.rangedCalls()
	if len(calls) != rangeParts {
		t.Fatalf("GetRange 调用 = %d 次，期望 %d 次", len(calls), rangeParts)
	}
	// 完成顺序故意与区间顺序相反，这里按 start 排序后再校验区间切分。
	sort.Slice(calls, func(i, j int) bool { return calls[i].start < calls[j].start })
	wantPart := testBlockBytes / rangeParts
	for i, c := range calls {
		wantStart := int64(i) * wantPart
		wantEnd := wantStart + wantPart - 1
		if c.start != wantStart || c.end != wantEnd {
			t.Fatalf("第 %d 个区间 = [%d,%d]，期望 [%d,%d]", i, c.start, c.end, wantStart, wantEnd)
		}
	}
	if peak := store.peak.Load(); peak < 2 {
		t.Fatalf("并发 GetRange 峰值 = %d，期望 >= 2（分片应并行）", peak)
	}
	if tmps, _ := listTmpFiles(casDir); len(tmps) != 0 {
		t.Fatalf("CAS 临时文件残留 = %v", tmps)
	}
	if empty, err := dirIsEmpty(rangeTempDir); err != nil || !empty {
		t.Fatalf("分片聚合临时目录未清空: %s (err=%v)", rangeTempDir, err)
	}
}

// failingRangeStore 在非首区间上先写一个字节再返回错误，用于验证失败路径
// 清理临时文件且不留下块。
type failingRangeStore struct {
	*objectstore.Mock
	data []byte
}

func (s *failingRangeStore) GetRange(ctx context.Context, key string, start, end int64, w io.Writer) error {
	if start > 0 {
		// 先写一部分再失败，模拟传输中途断开。
		if _, err := w.Write(s.data[start : start+1]); err != nil {
			return err
		}
		return errors.New("range transfer broken")
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(5 * time.Millisecond):
	}
	_, err := w.Write(s.data[start : end+1])
	return err
}

// TestFetchParallelRangesFailureCleansTemp 覆盖分片失败：Fetch 返回错误，
// 分片聚合临时文件与 CAS 临时文件都不残留，块目录为空。
func TestFetchParallelRangesFailureCleansTemp(t *testing.T) {
	store := &failingRangeStore{Mock: objectstore.NewMock(), data: make([]byte, testBlockBytes)}
	cs, casDir := newTestCAS(t)

	oldTempDir := rangeTempDir
	rangeTempDir = t.TempDir()
	t.Cleanup(func() { rangeTempDir = oldTempDir })

	f := New(store, cs, Options{MaxRetries: 1, RangeParallel: true})
	_, err := f.Fetch(context.Background(), "ds/blocks/broken#0", testBlockBytes)
	if err == nil {
		t.Fatal("Fetch 应返回错误")
	}
	if blocks, _ := listBlockFiles(casDir); len(blocks) != 0 {
		t.Fatalf("失败后残留块 = %v", blocks)
	}
	if tmps, _ := listTmpFiles(casDir); len(tmps) != 0 {
		t.Fatalf("失败后 CAS 临时文件残留 = %v", tmps)
	}
	if empty, err := dirIsEmpty(rangeTempDir); err != nil || !empty {
		t.Fatalf("失败后分片聚合临时目录未清空: %s (err=%v)", rangeTempDir, err)
	}
}

// countingStore 统计整对象 Get 与区间 Get 的调用次数，用于断言开关走的取数路径。
type countingStore struct {
	*objectstore.Mock
	mu     sync.Mutex
	gets   int
	ranges int
}

func (s *countingStore) Get(ctx context.Context, key string, w io.Writer) error {
	s.mu.Lock()
	s.gets++
	s.mu.Unlock()
	return s.Mock.Get(ctx, key, w)
}

func (s *countingStore) GetRange(ctx context.Context, key string, start, end int64, w io.Writer) error {
	s.mu.Lock()
	s.ranges++
	s.mu.Unlock()
	return s.Mock.GetRange(ctx, key, start, end, w)
}

func (s *countingStore) counts() (gets, ranges int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.gets, s.ranges
}

// TestFetchRangeParallelSwitch 覆盖并行下载开关：打开时每个块拆成 rangeParts 条
// Range GET 并行取，关闭时整块走一条 Get。块大小不参与判定。
func TestFetchRangeParallelSwitch(t *testing.T) {
	content := []byte("abcdefgh")
	key := "ds/blocks/switch#0"

	t.Run("开关打开走并行", func(t *testing.T) {
		store := &countingStore{Mock: objectstore.NewMock()}
		seed(t, store.Mock, key, content)
		cs, _ := newTestCAS(t)

		oldTempDir := rangeTempDir
		rangeTempDir = t.TempDir()
		t.Cleanup(func() { rangeTempDir = oldTempDir })

		f := New(store, cs, Options{RangeParallel: true})
		fp, err := f.Fetch(context.Background(), key, int64(len(content)))
		if err != nil {
			t.Fatalf("Fetch: %v", err)
		}
		if want := fpOf(content); fp != want {
			t.Fatalf("fp = %s, want %s", fp, want)
		}
		if got := readBlock(t, cs, fp); !bytes.Equal(got, content) {
			t.Fatalf("块内容 = %q, want %q", got, content)
		}
		gets, ranges := store.counts()
		if ranges != rangeParts {
			t.Fatalf("GetRange 调用 = %d 次，期望 %d 次（开关打开走并行路径）", ranges, rangeParts)
		}
		if gets != 0 {
			t.Fatalf("Get 调用 = %d 次，期望 0 次（并行路径不发整对象 GET）", gets)
		}
	})

	t.Run("开关关闭走单条 GET", func(t *testing.T) {
		store := &countingStore{Mock: objectstore.NewMock()}
		seed(t, store.Mock, key, content)
		cs, _ := newTestCAS(t)

		oldTempDir := rangeTempDir
		rangeTempDir = t.TempDir()
		t.Cleanup(func() { rangeTempDir = oldTempDir })

		f := New(store, cs, Options{})
		fp, err := f.Fetch(context.Background(), key, int64(len(content)))
		if err != nil {
			t.Fatalf("Fetch: %v", err)
		}
		if want := fpOf(content); fp != want {
			t.Fatalf("fp = %s, want %s", fp, want)
		}
		if got := readBlock(t, cs, fp); !bytes.Equal(got, content) {
			t.Fatalf("块内容 = %q, want %q", got, content)
		}
		gets, ranges := store.counts()
		if ranges != 0 {
			t.Fatalf("GetRange 调用 = %d 次，期望 0 次（开关关闭不发区间 GET）", ranges)
		}
		if gets != 1 {
			t.Fatalf("Get 调用 = %d 次，期望 1 次（开关关闭走整对象 GET）", gets)
		}
	})
}

// dirIsEmpty 返回 dir 是否为空目录（用于断言临时文件已清理）。
func dirIsEmpty(dir string) (bool, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return false, err
	}
	return len(entries) == 0, nil
}
