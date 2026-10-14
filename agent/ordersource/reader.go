package ordersource

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
	"io/fs"
	"log/slog"
	"os"
	"sync"
	"syscall"
	"time"
)

// ring 布局常量(design §6.2,冻结;小端)。
const (
	ringMagic      = "TCO1" // 偏移 0,4B
	ringVersion    = 1      // 偏移 4,2B
	ringHeaderSize = 64     // 头固定 64B(偏移 32..64 保留)
	slotHeaderSize = 12     // 每槽头:seq int64 + len int32,其后为 payload
)

// 几何 sanity 上限(decisions D22):越界视为 invalid,按 version 类计格式错误。
const (
	minSlotSize  = 16
	maxSlotSize  = 64 << 20 // 64 MiB
	maxSlotCount = 1024
)

// pollInterval 是 Reader 内部 poll 周期(D22:200ms;发现由 svc 1s 目录轮询承担)。
// 包内变量,白盒测试可改小。
var pollInterval = 200 * time.Millisecond

// ringHeader 是 64B 头的解析结果(仅保留读端用到的字段)。
type ringHeader struct {
	generation int64
	writeSeq   int64
	slotSize   int32
	slotCount  int32
}

// parseHeader 解析头字节;kind 非空表示格式错误,取值 errKindMagic / errKindVersion:
// magic 读不出或不符 → magic;version / 几何非法 → version(D22 计数口径)。
func parseHeader(b []byte) (ringHeader, string) {
	if len(b) < len(ringMagic) || string(b[:len(ringMagic)]) != ringMagic {
		return ringHeader{}, errKindMagic
	}
	if len(b) < ringHeaderSize {
		// magic 可读但头不完整:version / 几何无法校验
		return ringHeader{}, errKindVersion
	}
	if v := binary.LittleEndian.Uint16(b[4:6]); v != ringVersion {
		return ringHeader{}, errKindVersion
	}
	slotSize := int32(binary.LittleEndian.Uint32(b[24:28]))
	slotCount := int32(binary.LittleEndian.Uint32(b[28:32]))
	if slotSize < minSlotSize || slotSize > maxSlotSize || slotCount < 1 || slotCount > maxSlotCount {
		return ringHeader{}, errKindVersion
	}
	return ringHeader{
		generation: int64(binary.LittleEndian.Uint64(b[8:16])),
		writeSeq:   int64(binary.LittleEndian.Uint64(b[16:24])),
		slotSize:   slotSize,
		slotCount:  slotCount,
	}, ""
}

// subscription 是单个订阅者:缓冲 1、丢旧保新(latest-wins;约定 8)。
type subscription struct {
	ch chan ConsumeOrder
}

// Reader 是 OrderSource 的共享内存读端(design §5.3;由 svc 装配)。所有可变状态在 mu 下访问;
// poll goroutine 由 NewReader 启动、Close 停止。
type Reader struct {
	ringPath string
	watchDir string // svc 侧发现目录(D22:发现 = svc 轮询);本包只读 ringPath

	mu      sync.Mutex
	done    chan struct{} // Close 时关闭:通知 poll goroutine 退出
	stopped chan struct{} // poll goroutine 退出后关闭(Close 等待)
	closed  bool

	f    *os.File // 当前 ring 文件;nil = 未打开(missing / 未发现)
	data []byte   // mmap 映射,len 即映射长度;nil = 未映射(文件 < 64B 或 mmap 失败)

	missing bool // 文件当前不存在(M5 降级判定用;M2 只做状态标记)

	attached    bool   // 已解析到有效头(追读状态有效)
	lastGen     int64  // 上次有效头的 writer_generation
	lastReadSeq int64  // 已追读到的 write_seq
	badKind     string // 当前文件已计数的坏头 kind(""/magic/version:同一坏头状态只计一次)

	cur         ConsumeOrder
	hasCur      bool
	lastEmitted []byte // 上次对外推送(或启动初始快照)的载荷字节:按字节去重

	subs   map[uint64]*subscription
	nextID uint64
}

// OrderSource 实现断言(冻结接口)。
var _ OrderSource = (*Reader)(nil)

// NewReader 构造读端并做一次同步初始快照,随后启动内部 poll goroutine。
// ringPath 是 sidecar 的 ring 文件;watchDir 是 svc 侧发现目录(D22:发现 = svc 目录轮询
// *.ring 后为新文件构造 Reader),本包不扫目录。
//
// 初始快照按"不重放"约定:启动时读到的当前订单只置 cur 与去重基线,不向订阅通道推送
// (svc 侧以"订阅后先 CurrentOrder 再收事件"补齐)。
func NewReader(ringPath string, watchDir string) *Reader {
	r := &Reader{
		ringPath: ringPath,
		watchDir: watchDir,
		done:     make(chan struct{}),
		stopped:  make(chan struct{}),
		subs:     make(map[uint64]*subscription),
	}
	r.mu.Lock()
	r.pollOnceLocked(true)
	r.mu.Unlock()
	go r.pollLoop()
	return r
}

// pollLoop 周期轮询(包内 pollInterval);Close 后退出并关闭 stopped。
func (r *Reader) pollLoop() {
	defer close(r.stopped)
	for {
		r.mu.Lock()
		if r.closed {
			r.mu.Unlock()
			return
		}
		r.pollOnceLocked(false)
		r.mu.Unlock()
		select {
		case <-r.done:
			return
		case <-time.After(pollInterval):
		}
	}
}

// CurrentOrder 返回当前订单的深拷贝(chunks 复制,不暴露映射内存;约定 12)。
// 尚无任何推送、或 Reader 已关闭 → false。
func (r *Reader) CurrentOrder() (ConsumeOrder, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed || !r.hasCur {
		return ConsumeOrder{}, false
	}
	return copyOrder(r.cur), true
}

// Missing 报告 ring 文件当前是否不存在(M5 降级模式判定用;M2 只做状态标记与状态转移日志)。
func (r *Reader) Missing() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.missing
}

// SubscribeStageSwitch 订阅阶段切换事件(约定 8):
// channel 缓冲 1、丢旧保新(latest-wins),永不 close;ctx 取消 = 订阅终止(从订阅表移除,不 close channel);
// ctx 已取消 → 直接返回 ctx.Err();Reader 已关闭 → ErrClosed。
func (r *Reader) SubscribeStageSwitch(ctx context.Context) (<-chan ConsumeOrder, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return nil, ErrClosed
	}
	r.nextID++
	id := r.nextID
	sub := &subscription{ch: make(chan ConsumeOrder, 1)}
	r.subs[id] = sub
	r.mu.Unlock()

	go func() {
		select {
		case <-ctx.Done():
		case <-r.done:
		}
		r.unsubscribe(id)
	}()
	return sub.ch, nil
}

// unsubscribe 从订阅表移除订阅者(不 close 订阅 channel)。
func (r *Reader) unsubscribe(id uint64) {
	r.mu.Lock()
	delete(r.subs, id)
	r.mu.Unlock()
}

// Close 幂等关闭:停 poll goroutine、munmap、关 fd、移除全部订阅者(不 close 订阅 channel);
// 之后 CurrentOrder 返回 false、SubscribeStageSwitch 返回 ErrClosed。返回时 poll goroutine 已退出。
func (r *Reader) Close() {
	r.mu.Lock()
	if !r.closed {
		r.closed = true
		close(r.done)
		r.detachLocked() // munmap + 关 fd
		r.subs = nil     // 移除全部订阅者(不 close channel)
		r.cur, r.hasCur = ConsumeOrder{}, false
		r.lastEmitted = nil
	}
	r.mu.Unlock()
	<-r.stopped // 幂等:stopped 关闭后重复接收立即返回
}

// pollOnceLocked 执行一轮:刷新文件/映射 → 校验头 → 代际判定 → 追读 / 跳读 / 重读最新。
// suppress 为真时只更新状态不推送(NewReader 初始快照,约定 7:初始状态不重放)。
func (r *Reader) pollOnceLocked(suppress bool) {
	if r.closed {
		return
	}
	if !r.refreshFileLocked() {
		return
	}
	h, ok := r.parseHeaderLocked()
	if !ok {
		return
	}
	if r.data == nil {
		// 头可读但槽区不可映射(文件未长足 / mmap 失败):保留轮询,下轮重试。
		return
	}

	fresh := !r.attached || h.generation != r.lastGen
	if !fresh && h.writeSeq < r.lastReadSeq {
		// 防御(约定 13):同一代际 write_seq 回退 = 写端 bug → 重置状态跳最新。
		slog.Warn("ordersource: write_seq 回退(写端 bug),重置状态跳最新",
			"path", r.ringPath, "generation", h.generation,
			"write_seq", h.writeSeq, "last_read_seq", r.lastReadSeq)
		fresh = true
	}
	if fresh {
		r.resyncLocked(h, suppress)
		return
	}
	r.followLocked(h)
}

// resyncLocked 丢弃追读位置、跳到最新:首次接入 / 代际变化(约定 6)/ 写端回退后的共同路径。
// 只读最新槽,按字节去重决定是否推送;suppress 仅用于 NewReader 初始快照。
func (r *Reader) resyncLocked(h ringHeader, suppress bool) {
	r.attached = true
	r.lastGen = h.generation
	r.lastReadSeq = h.writeSeq // 跳过历史:阶段切换只关心最新顺序
	if h.writeSeq <= 0 {
		return
	}
	order, raw, out := r.readSlotLocked(h, h.writeSeq)
	switch out {
	case slotOK:
		r.acceptLocked(order, raw, suppress)
	case slotCorrupt:
		// 最新槽载荷损坏:跳过(等后续序号);lastReadSeq 已在最新位。
	case slotRetry:
		// 槽暂不可读(映射未覆盖 / seq 未落定):保持"已跳到最新",等后续序号。
	}
}

// followLocked 追读:target = 头 write_seq;落后超过 slot_count → 跳读最新;
// 否则顺序读 (lastReadSeq, target],槽 seq 不符(陈旧)/ len 非法 → 停读该槽等下一轮,
// 载荷 JSON 解析失败 → 跳过该槽继续追读(约定 5;不计数)。
func (r *Reader) followLocked(h ringHeader) {
	target := h.writeSeq
	if target <= r.lastReadSeq {
		return
	}
	if target > r.lastReadSeq+int64(h.slotCount) {
		order, raw, out := r.readSlotLocked(h, target)
		switch out {
		case slotOK:
			r.acceptLocked(order, raw, false)
			r.lastReadSeq = target
		case slotCorrupt:
			r.lastReadSeq = target // 损坏槽跳过,避免反复重试同一槽
		case slotRetry:
			// 保持追读位置,下一轮重试(写端继续推进时最坏自然跳读)。
		}
		return
	}
	for seq := r.lastReadSeq + 1; seq <= target; seq++ {
		order, raw, out := r.readSlotLocked(h, seq)
		switch out {
		case slotOK:
			r.acceptLocked(order, raw, false)
			r.lastReadSeq = seq
		case slotCorrupt:
			r.lastReadSeq = seq // 跳过该槽继续追读
		default:
			return // 陈旧 / 未就绪:停止本轮
		}
	}
}

// slotOutcome 是槽读取结果。
type slotOutcome int

const (
	slotOK      slotOutcome = iota // 读取并解析成功
	slotRetry                      // 暂时不可读(映射未覆盖 / seq 不符 / len 非法 / 半写):调用方保留状态下轮重试
	slotCorrupt                    // seq 匹配但载荷 JSON 解析失败:调用方跳过该 seq
)

// readSlotLocked 读取 seq 对应的槽。全部用 binary.LittleEndian 拷贝读取,不用 unsafe(约定 13);
// 越界只判映射长度,不触页错误。
func (r *Reader) readSlotLocked(h ringHeader, seq int64) (ConsumeOrder, []byte, slotOutcome) {
	slotSize := int64(h.slotSize)
	off := int64(ringHeaderSize) + ((seq-1)%int64(h.slotCount))*slotSize
	if off+slotHeaderSize > int64(len(r.data)) {
		return ConsumeOrder{}, nil, slotRetry // 映射未覆盖该槽(文件尚未长到位)
	}
	if gotSeq := int64(binary.LittleEndian.Uint64(r.data[off : off+8])); gotSeq != seq {
		return ConsumeOrder{}, nil, slotRetry // 陈旧槽 / 写入未落定
	}
	n := int64(int32(binary.LittleEndian.Uint32(r.data[off+8 : off+12])))
	if n <= 0 || n > slotSize-slotHeaderSize {
		return ConsumeOrder{}, nil, slotRetry // 无效长度:不推送非法载荷
	}
	start := off + slotHeaderSize
	end := start + n
	if end > int64(len(r.data)) {
		return ConsumeOrder{}, nil, slotRetry // 载荷尚未完整落入文件
	}
	raw := r.data[start:end]
	var order ConsumeOrder
	if err := json.Unmarshal(raw, &order); err != nil {
		slog.Warn("ordersource: 槽载荷 JSON 解析失败,跳过该槽",
			"path", r.ringPath, "seq", seq, "err", err)
		return ConsumeOrder{}, nil, slotCorrupt
	}
	return order, raw, slotOK
}

// acceptLocked 接受一份已解析订单:更新当前订单;suppress(初始快照)只置去重基线不推送;
// 其余按载荷字节去重(相同不推,约定 7)。
func (r *Reader) acceptLocked(order ConsumeOrder, raw []byte, suppress bool) {
	r.cur = order
	r.hasCur = true
	if suppress {
		r.lastEmitted = append(r.lastEmitted[:0], raw...)
		return
	}
	if bytes.Equal(raw, r.lastEmitted) {
		return
	}
	r.lastEmitted = append(r.lastEmitted[:0], raw...)
	r.deliverLocked(order)
}

// deliverLocked 向全部订阅者投递(丢旧保新:非阻塞 send,满则 drain 后送最新;约定 8)。
// 每个订阅者收到独立深拷贝,订阅方修改不影响 Reader 内部状态与其他订阅者。
func (r *Reader) deliverLocked(order ConsumeOrder) {
	for _, sub := range r.subs {
		o := copyOrder(order)
		select {
		case sub.ch <- o:
		default:
			select {
			case <-sub.ch:
			default:
			}
			select {
			case sub.ch <- o:
			default:
			}
		}
	}
}

// copyOrder 返回订单深拷贝(chunks slice 复制)。
func copyOrder(o ConsumeOrder) ConsumeOrder {
	out := o
	if o.Chunks != nil {
		out.Chunks = append([]ChunkRef(nil), o.Chunks...)
	}
	return out
}

// refreshFileLocked 确保 r.f 指向 ring 当前 inode、r.data 覆盖当前文件长度:
// 文件不存在 → missing 状态(下轮继续 discover);删除重建 → 重新 open/mmap;
// 长大 → munmap + 重新 mmap。返回 false 表示本轮无法读取(调用方直接返回)。
func (r *Reader) refreshFileLocked() bool {
	st, err := os.Stat(r.ringPath)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			r.markMissingLocked()
			return false
		}
		slog.Warn("ordersource: stat ring 失败", "path", r.ringPath, "err", err)
		return false
	}
	if r.f == nil {
		return r.openLocked()
	}
	fst, err := r.f.Stat()
	if err != nil {
		slog.Warn("ordersource: fstat ring 失败", "path", r.ringPath, "err", err)
		r.markMissingLocked()
		return false
	}
	if !os.SameFile(st, fst) {
		// 文件已删除重建:fd 仍指向旧 inode,重新 open/mmap(D22:代际检测兜底)。
		r.detachLocked()
		return r.openLocked()
	}
	if int64(len(r.data)) != st.Size() {
		// 尺寸变化(写端只增不缩)→ munmap + 重新 mmap
		r.remapLocked(st.Size())
	}
	return true
}

// openLocked 打开 ring 文件并把映射刷到当前长度。新打开的文件重置坏头计数状态
// (同一文件同一坏头状态只计一次;重开视为新状态,复验失败再计)。
func (r *Reader) openLocked() bool {
	f, err := os.OpenFile(r.ringPath, os.O_RDONLY, 0)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			r.markMissingLocked()
			return false
		}
		slog.Warn("ordersource: 打开 ring 失败", "path", r.ringPath, "err", err)
		return false
	}
	st, err := f.Stat()
	if err != nil {
		slog.Warn("ordersource: fstat ring 失败", "path", r.ringPath, "err", err)
		_ = f.Close()
		return false
	}
	r.f = f
	r.attached = false
	r.badKind = ""
	if r.missing {
		r.missing = false
		slog.Info("ordersource: ring 文件出现,开始读取", "path", r.ringPath, "watch_dir", r.watchDir)
	}
	r.remapLocked(st.Size())
	return true
}

// remapLocked 把映射刷到 size:先 munmap;size < 64(头都读不出)或 mmap 失败时保持未映射,下轮重试。
func (r *Reader) remapLocked(size int64) {
	if r.data != nil {
		_ = syscall.Munmap(r.data)
		r.data = nil
	}
	if r.f == nil || size < ringHeaderSize {
		return
	}
	data, err := syscall.Mmap(int(r.f.Fd()), 0, int(size), syscall.PROT_READ, syscall.MAP_SHARED)
	if err != nil {
		slog.Warn("ordersource: mmap ring 失败", "path", r.ringPath, "size", size, "err", err)
		return
	}
	r.data = data
}

// detachLocked 解除当前文件(munmap + 关 fd)并重置附着状态;坏头计数随新状态重新开始。
func (r *Reader) detachLocked() {
	if r.data != nil {
		_ = syscall.Munmap(r.data)
		r.data = nil
	}
	if r.f != nil {
		_ = r.f.Close()
		r.f = nil
	}
	r.attached = false
	r.badKind = ""
}

// markMissingLocked 进入 missing 状态并记录状态转移日志(M5 降级模式用)。
func (r *Reader) markMissingLocked() {
	if !r.missing {
		slog.Info("ordersource: ring 文件不存在(Missing,下轮继续尝试发现)",
			"path", r.ringPath, "watch_dir", r.watchDir)
	}
	r.missing = true
	r.detachLocked()
}

// parseHeaderLocked 读取并校验当前文件头;返回 (h, true) 表示头有效。
// 格式非法:忽略该文件并按 kind 计一次错误(D23:同一坏头状态只计一次)。
func (r *Reader) parseHeaderLocked() (ringHeader, bool) {
	var hb []byte
	if r.data != nil {
		hb = r.data[:ringHeaderSize]
	} else {
		// 文件不足 64B 或 mmap 失败:读前缀判定 kind(magic 读不出 → magic,否则 → version)。
		buf := make([]byte, ringHeaderSize)
		n, err := r.f.ReadAt(buf, 0)
		if err != nil && !errors.Is(err, io.EOF) {
			slog.Warn("ordersource: 读 ring 头失败", "path", r.ringPath, "err", err)
			return ringHeader{}, false
		}
		hb = buf[:n]
	}
	h, kind := parseHeader(hb)
	if kind != "" {
		r.countBadHeaderLocked(kind)
		r.attached = false
		return ringHeader{}, false
	}
	r.badKind = ""
	return h, true
}

// countBadHeaderLocked 记录坏头(D23):同一文件同一坏头状态只计一次。
func (r *Reader) countBadHeaderLocked(kind string) {
	if r.badKind == kind {
		return
	}
	r.badKind = kind
	countFormatError(kind)
	slog.Warn("ordersource: ring 头格式非法,忽略该文件",
		"path", r.ringPath, "reason", kind)
}
