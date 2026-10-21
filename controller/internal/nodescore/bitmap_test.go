package nodescore

import (
	"errors"
	"testing"
)

func TestDecodeReadinessBitOrder(t *testing.T) {
	// Requested chunks 0..9; bits 0,3,4,9 are ready.
	bitmap := []byte{0b00011001, 0b00000010}
	got, err := DecodeReadiness(bitmap, 10, 4)
	if err != nil {
		t.Fatalf("DecodeReadiness: %v", err)
	}
	wantBits := []bool{true, false, false, true, true, false, false, false, false, true}
	if len(got.Bits) != len(wantBits) {
		t.Fatalf("bits = %d, want %d", len(got.Bits), len(wantBits))
	}
	for i := range wantBits {
		if got.Bits[i] != wantBits[i] {
			t.Fatalf("bit %d = %v, want %v", i, got.Bits[i], wantBits[i])
		}
	}
	if got.ReadyInWindow != 2 {
		t.Fatalf("ready in window = %d, want 2", got.ReadyInWindow)
	}
	if got.ReadyTotal != 4 {
		t.Fatalf("ready total = %d, want 4", got.ReadyTotal)
	}
}

func TestDecodeReadinessNonByteAligned(t *testing.T) {
	// 9 chunks need 2 bytes; padding bits 9..15 are set but must be ignored.
	got, err := DecodeReadiness([]byte{0xFF, 0xFF}, 9, 9)
	if err != nil {
		t.Fatalf("DecodeReadiness: %v", err)
	}
	if got.ReadyTotal != 9 {
		t.Fatalf("ready total = %d, want 9", got.ReadyTotal)
	}
	if len(got.Bits) != 9 {
		t.Fatalf("bits = %d, want 9", len(got.Bits))
	}
}

func TestDecodeReadinessAllZero(t *testing.T) {
	got, err := DecodeReadiness(make([]byte, 1), 8, 8)
	if err != nil {
		t.Fatalf("DecodeReadiness: %v", err)
	}
	if got.ReadyInWindow != 0 || got.ReadyTotal != 0 {
		t.Fatalf("readiness = %+v, want all zero", got)
	}
}

func TestDecodeReadinessAllOne(t *testing.T) {
	got, err := DecodeReadiness([]byte{0xFF}, 8, 8)
	if err != nil {
		t.Fatalf("DecodeReadiness: %v", err)
	}
	if got.ReadyInWindow != 8 || got.ReadyTotal != 8 {
		t.Fatalf("readiness = %+v, want 8/8", got)
	}
}

func TestDecodeReadinessWindowBoundaries(t *testing.T) {
	bitmap := []byte{0xFF, 0xFF} // 16 requested chunks all ready
	for _, tc := range []struct {
		k    int32
		want int32
	}{
		{k: 1, want: 1},
		{k: 7, want: 7},
		{k: 8, want: 8},
		{k: 9, want: 9},
		{k: 16, want: 16},
		{k: 20, want: 16}, // clamped to requested
	} {
		got, err := DecodeReadiness(bitmap, 16, tc.k)
		if err != nil {
			t.Fatalf("K=%d: %v", tc.k, err)
		}
		if got.ReadyInWindow != tc.want {
			t.Fatalf("K=%d: ready in window = %d, want %d", tc.k, got.ReadyInWindow, tc.want)
		}
		if got.ReadyTotal != 16 {
			t.Fatalf("K=%d: ready total = %d, want 16", tc.k, got.ReadyTotal)
		}
	}
}

func TestDecodeReadinessLengthMismatch(t *testing.T) {
	for _, tc := range []struct {
		chunks int32
		bitmap []byte
	}{
		{chunks: 1, bitmap: []byte{}},
		{chunks: 8, bitmap: []byte{0xFF, 0xFF}},
		{chunks: 9, bitmap: []byte{0xFF}},
		{chunks: 0, bitmap: []byte{0x01}},
	} {
		if _, err := DecodeReadiness(tc.bitmap, tc.chunks, tc.chunks); !errors.Is(err, ErrBitmapLength) {
			t.Fatalf("chunks=%d len=%d err = %v, want ErrBitmapLength", tc.chunks, len(tc.bitmap), err)
		}
	}
	if _, err := DecodeReadiness(nil, -1, 0); err == nil {
		t.Fatal("negative chunk count accepted")
	}
}

func TestCountHelpers(t *testing.T) {
	bits := []bool{true, false, true, true, false}
	if got := CountReady(bits); got != 3 {
		t.Fatalf("CountReady = %d, want 3", got)
	}
	if got := CountReadyInWindow(bits, 2); got != 1 {
		t.Fatalf("CountReadyInWindow(2) = %d, want 1", got)
	}
	if got := CountReadyInWindow(bits, 99); got != 3 {
		t.Fatalf("CountReadyInWindow(clamped) = %d, want 3", got)
	}
	if got := Popcount([]byte{0x0F, 0xF0}); got != 8 {
		t.Fatalf("Popcount = %d, want 8", got)
	}
}
