// Package nodescore holds the pure readiness/score helpers used by the warmup
// controller (docs/8.1 design.md §7.3). Bitmap decoding is M5; the decay score
// implementation is M8.
package nodescore

import (
	"errors"
	"fmt"
	"math/bits"
)

// ErrBitmapLength reports a ready_bitmap whose length does not match
// ceil(requested/8). Polling treats it as a parse failure and leaves the
// readiness table untouched (plan M5 criterion ②).
var ErrBitmapLength = errors.New("nodescore: ready bitmap length mismatch")

// Readiness is the decoded view of one Agent ready_bitmap.
type Readiness struct {
	// Bits is indexed by request chunk order: Bits[i] is chunks[i].
	Bits []bool
	// ReadyInWindow counts ready bits in [0, Window).
	ReadyInWindow int32
	// ReadyTotal counts ready bits among all requested chunks.
	ReadyTotal int32
}

// DecodeReadiness validates and decodes a ready_bitmap. requested is the
// number of chunks in the corresponding preheat request (K is not applied
// here); window is the Pod's startup window K_i. Padding bits past requested
// are ignored, so a non-multiple-of-8 request is handled correctly.
func DecodeReadiness(bitmap []byte, requested, window int32) (Readiness, error) {
	if requested < 0 {
		return Readiness{}, fmt.Errorf("nodescore: negative chunk count %d", requested)
	}
	want := int((requested + 7) / 8)
	if len(bitmap) != want {
		return Readiness{}, fmt.Errorf("%w: got %d bytes, want %d for %d chunks",
			ErrBitmapLength, len(bitmap), want, requested)
	}
	if window < 0 {
		window = 0
	}
	if window > requested {
		window = requested
	}

	out := Readiness{Bits: make([]bool, requested)}
	for i := int32(0); i < requested; i++ {
		if bitmap[i/8]&(1<<(uint(i)%8)) != 0 {
			out.Bits[i] = true
			out.ReadyTotal++
			if i < window {
				out.ReadyInWindow++
			}
		}
	}
	return out, nil
}

// CountReady returns the number of set bits.
func CountReady(bits []bool) int32 {
	var n int32
	for _, bit := range bits {
		if bit {
			n++
		}
	}
	return n
}

// CountReadyInWindow returns the number of set bits in [0, window).
func CountReadyInWindow(bits []bool, window int32) int32 {
	if window <= 0 {
		return 0
	}
	if window > int32(len(bits)) {
		window = int32(len(bits))
	}
	var n int32
	for i := int32(0); i < window; i++ {
		if bits[i] {
			n++
		}
	}
	return n
}

// Popcount returns the number of set bits in a byte slice. It is exported for
// callers that only need the total count and not the per-chunk bits.
func Popcount(bitmap []byte) int32 {
	var n int32
	for _, b := range bitmap {
		n += int32(bits.OnesCount8(b))
	}
	return n
}
