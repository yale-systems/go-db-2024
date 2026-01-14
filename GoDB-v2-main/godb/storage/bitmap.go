package storage

import (
	"unsafe"

	"mit.edu/dsg/godb/common"
)

// Bitmap provides a convenient interface for manipulating bits in a byte slice.
// It does not own the underlying bytes; instead, it provides a structured view over
// an existing buffer (e.g., a database page).
//
// The implementation should be optimized for performance by performing word-level (uint64)
// operations during scans to skip full blocks of set bits.
type Bitmap struct {
	words   []uint64
	numBits int
}

// AsBitmap creates a Bitmap view over the provided byte slice.
//
// Constraints:
// 1. data must be aligned to 8 bytes to allow safe casting to uint64.
// 2. data must be large enough to contain numBits (rounded up to the nearest 8-byte word).
func AsBitmap(data []byte, numBits int) Bitmap {
	common.Assert(common.AlignedTo8(len(data)), "Bitmap bytes length must be aligned to 8")

	numWords := (numBits + 63) / 64
	common.Assert(len(data) >= numWords*8, "bitmap buffer too small")

	ptr := unsafe.Pointer(&data[0])
	// Slice reference cast to uint64
	words := unsafe.Slice((*uint64)(ptr), numWords)

	return Bitmap{
		words:   words,
		numBits: numBits,
	}
}

// SetBit sets the bit at index i to the given value.
// Returns the previous value of the bit.
func (b *Bitmap) SetBit(i int, on bool) (originalValue bool) {
	// <silentstrip lab1|lab2|lab3|lab4>
	common.Assert(i >= 0 && i < b.numBits, "indexing out of bounds")
	wordIdx := i / 64
	bitIdx := uint(i % 64)
	mask := uint64(1) << bitIdx

	ptr := &b.words[wordIdx]
	originalValue = (*ptr & mask) != 0
	if on {
		*ptr |= mask
	} else {
		*ptr &^= mask
	}
	return originalValue
	// </silentstrip>
	// <insert lab1|lab2|lab3|lab4>
	// panic("unimplemented")
	// </insert>
}

// LoadBit returns the value of the bit at index i.
func (b *Bitmap) LoadBit(i int) bool {
	// <silentstrip lab1|lab2|lab3|lab4>
	common.Assert(i >= 0 && i < b.numBits, "indexing out of bounds")
	wordIdx := i / 64
	bitIdx := uint(i % 64)
	return (b.words[wordIdx] & (1 << bitIdx)) != 0
	// </silentstrip>
	// <insert lab1|lab2|lab3|lab4>
	// panic("unimplemented")
	// </insert>
}

// FindFirstZero searches for the first bit set to 0 (false) in the bitmap.
// It begins the search at startHint and scans to the end of the bitmap.
// If no zero bit is found, it wraps around and scans from the beginning (index 0)
// up to startHint.
//
// Returns the index of the first zero bit found, or -1 if the bitmap is entirely full.
func (b *Bitmap) FindFirstZero(startHint int) int {
	// <silentstrip lab1|lab2|lab3|lab4>
	if r := b.findFirstZeroInRange(startHint, b.numBits); r != -1 {
		return r
	}
	return b.findFirstZeroInRange(0, startHint)
	// </silentstrip>
	// <insert lab1|lab2|lab3|lab4>
	// panic("unimplemented")
	// </insert>
}

// <silentstrip lab1|lab2|lab3|lab4>
func (b *Bitmap) findFirstZeroInRange(start, end int) int {
	// <silentstrip lab1|lab2|lab3|lab4>
	common.Assert(start >= 0 && start <= end && end <= b.numBits, "invalid Bitmap range")
	startWord := start / 64
	endWord := (end - 1) / 64

	for i := startWord; i <= endWord; i++ {
		word := b.words[i]

		// If word is all 1s, skip entirely
		if word == ^uint64(0) {
			continue
		}

		bitStart, bitEnd := 0, 64
		if i == startWord {
			bitStart = start % 64
		}
		if i == endWord {
			limit := end % 64
			if limit != 0 {
				bitEnd = limit
			}
		}

		// Check bits manually in this word
		for j := bitStart; j < bitEnd; j++ {
			if (word & (1 << j)) == 0 {
				totalIdx := i*64 + j
				return totalIdx
			}
		}
	}
	return -1
}

// </silentstrip>
