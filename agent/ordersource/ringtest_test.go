package ordersource

import (
	"encoding/binary"
	"encoding/json"
	"os"
	"testing"
)

// 本文件是测试专用 ring 写端(模拟 sidecar,design §6.2 / decisions D22 写序):
// 写头 / 写槽 / 推 commit(先整槽后头 write_seq)。生产代码不引用本文件。

// buildRingHeader 构造 64B 头(默认 magic/version/几何合法;坏头用例再逐字段破坏)。
func buildRingHeader(gen, seq int64, slotSize, slotCount uint32) [ringHeaderSize]byte {
	var h [ringHeaderSize]byte
	copy(h[0:4], ringMagic)
	binary.LittleEndian.PutUint16(h[4:6], ringVersion)
	binary.LittleEndian.PutUint64(h[8:16], uint64(gen))
	binary.LittleEndian.PutUint64(h[16:24], uint64(seq))
	binary.LittleEndian.PutUint32(h[24:28], slotSize)
	binary.LittleEndian.PutUint32(h[28:32], slotCount)
	return h
}

// writeRingFile 直接落盘一段字节(坏头 / 短文件用例用)。
func writeRingFile(t *testing.T, path string, b []byte) {
	t.Helper()
	if err := os.WriteFile(path, b, 0o644); err != nil {
		t.Fatalf("写测试 ring 文件 %s: %v", path, err)
	}
}

// slotOffset 计算槽偏移,与读端公式一致:64 + ((seq-1) mod slot_count) * slot_size。
func slotOffset(seq int64, slotSize, slotCount int32) int64 {
	return ringHeaderSize + ((seq-1)%int64(slotCount))*int64(slotSize)
}

// ringWriter 模拟 sidecar 写端:先整槽后头(commit 标记 = 头 write_seq 最后写)。
type ringWriter struct {
	t         *testing.T
	f         *os.File
	gen       int64
	seq       int64 // 已 commit 的 write_seq
	slotSize  int32
	slotCount int32
}

// newRingWriter 创建 ring 文件并写入初始头(gen、write_seq=0、几何),文件按
// D22 写端策略 ftruncate 到 64+slot_size(只增不缩的起点)。
func newRingWriter(t *testing.T, path string, gen int64, slotSize, slotCount int32) *ringWriter {
	t.Helper()
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0o644)
	if err != nil {
		t.Fatalf("创建测试 ring 文件 %s: %v", path, err)
	}
	t.Cleanup(func() { _ = f.Close() })
	w := &ringWriter{t: t, f: f, gen: gen, slotSize: slotSize, slotCount: slotCount}
	w.writeHeader()
	if err := f.Truncate(ringHeaderSize + int64(slotSize)); err != nil {
		t.Fatalf("ftruncate 测试 ring 文件: %v", err)
	}
	return w
}

// writeHeader 重写 64B 头(commit = write_seq 最后写,由调用顺序保证)。
func (w *ringWriter) writeHeader() {
	w.t.Helper()
	h := buildRingHeader(w.gen, w.seq, uint32(w.slotSize), uint32(w.slotCount))
	if _, err := w.f.WriteAt(h[:], 0); err != nil {
		w.t.Fatalf("写 ring 头: %v", err)
	}
}

// WriteOrder 写一份订单:先整槽、后推头(commit);返回 seq。
func (w *ringWriter) WriteOrder(o ConsumeOrder) int64 {
	w.t.Helper()
	payload, err := json.Marshal(o)
	if err != nil {
		w.t.Fatalf("序列化 ConsumeOrder: %v", err)
	}
	w.seq++
	w.writeSlot(w.seq, int32(len(payload)), payload)
	w.writeHeader()
	return w.seq
}

// WriteRawSlot 写整槽但不推头(未提交 / 半写模拟:commit 未达 → 读端不可见)。
func (w *ringWriter) WriteRawSlot(seq int64, payload []byte) {
	w.t.Helper()
	w.writeSlot(seq, int32(len(payload)), payload)
}

// WriteSlotBadLen 写槽头与指定的(非法)len,只落少量载荷字节(坏 len 用例)。
func (w *ringWriter) WriteSlotBadLen(seq int64, declaredLen int32, payload []byte) {
	w.t.Helper()
	w.writeSlot(seq, declaredLen, payload)
}

// CommitSeq 推头 write_seq=seq(commit 标记最后写)。
func (w *ringWriter) CommitSeq(seq int64) {
	w.t.Helper()
	w.seq = seq
	w.writeHeader()
}

// SetGeneration 改代际并立即重写头(模拟 sidecar 重启;write_seq 续增口径不变)。
func (w *ringWriter) SetGeneration(gen int64) {
	w.t.Helper()
	w.gen = gen
	w.writeHeader()
}

// writeSlot 写整槽(必要处先扩文件,模拟写端"只增不缩"的 ftruncate 至覆盖该槽)。
func (w *ringWriter) writeSlot(seq int64, declaredLen int32, payload []byte) {
	w.t.Helper()
	off := slotOffset(seq, w.slotSize, w.slotCount)
	need := off + int64(w.slotSize)
	if st, err := w.f.Stat(); err != nil {
		w.t.Fatalf("fstat 测试 ring 文件: %v", err)
	} else if st.Size() < need {
		if err := w.f.Truncate(need); err != nil {
			w.t.Fatalf("ftruncate 测试 ring 文件: %v", err)
		}
	}
	buf := make([]byte, slotHeaderSize+len(payload))
	binary.LittleEndian.PutUint64(buf[0:8], uint64(seq))
	binary.LittleEndian.PutUint32(buf[8:12], uint32(declaredLen))
	copy(buf[slotHeaderSize:], payload)
	if _, err := w.f.WriteAt(buf, off); err != nil {
		w.t.Fatalf("写 ring 槽: %v", err)
	}
}

// mustMarshal 序列化 ConsumeOrder(测试内 payload 直信用)。
func mustMarshal(t *testing.T, o ConsumeOrder) []byte {
	t.Helper()
	b, err := json.Marshal(o)
	if err != nil {
		t.Fatalf("序列化 ConsumeOrder: %v", err)
	}
	return b
}
