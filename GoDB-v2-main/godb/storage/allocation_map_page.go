package storage

import (
	"mit.edu/dsg/godb/common"
)

// AllocationMapPage represents a special page type used to track free space in a DBFile. This page acts as a hint for
// the table heap file to find space to insert a new tuple. There is always an allocation map page every
// AllocationMapPageSlots pages that tracks metadata for the next chunk of actual heap pages.
//
// Layout:
// Unlike HeapPages, which store Tuples, an AllocationMapPage is essentially a giant Bitmap.
// - Each bit 'i' in the bitmap corresponds to the status of the 'i-th' page relative to this map (including itself).
// - 0 indicates the page has space.
// - 1 indicates the page is full.
type AllocationMapPage struct {
	*PageFrame
	Bitmap Bitmap
}

// AllocationMapPageSlots defines the number of pages a single allocation map can track.
const AllocationMapPageSlots = common.PageSize*8 - 8

// InitializeAllocationMapPage initializes a raw page frame to function as an allocation map.
func InitializeAllocationMapPage(frame *PageFrame) {

	// For simplicity, bits correspond to offsets from the allocation map page.
	// The first bit therefore tracks the allocation map itself and is therefore always marked full.
	bitmap := AsBitmap(frame.Bytes[pageOffsetLSN:], AllocationMapPageSlots)
	bitmap.SetBit(0, true)
}

// AsAllocationMapPage casts a generic PageFrame into an AllocationMapPage. The caller is responsible for ensuring
// that the underlying page has been initialized via InitializeAllocationMapPage.
func (frame *PageFrame) AsAllocationMapPage() AllocationMapPage {
	result := AllocationMapPage{
		PageFrame: frame,
		Bitmap:    AsBitmap(frame.Bytes[pageOffsetLSN:], AllocationMapPageSlots),
	}
	common.Assert(result.Bitmap.LoadBit(0), "page ReadFromFullTuple should be initialized")
	return result
}

// FindFirstFreePage scans the bitmap to find the first page that has space. Search starts from startOffset, which
// allows for optimizations (e.g., "last known free") or round-robin allocation strategies. Returns -1 of the entire
// map is full.
func (amp AllocationMapPage) FindFirstFreePage(startOffset int) int {
	common.Assert(startOffset >= 0 && startOffset < amp.Bitmap.numBits, "indexing out of bounds")
	return amp.Bitmap.FindFirstZero(startOffset)

}

// MarkPageFull updates the allocation status of a specific page. Return true if the status actually changed (e.g., was
// 0, now 1), false if the status was already what we requested.
func (amp AllocationMapPage) MarkPageFull(relativePageOffset int, full bool) (flipped bool) {
	common.Assert(relativePageOffset >= 0 && relativePageOffset < AllocationMapPageSlots, "indexing out of bounds")
	return amp.Bitmap.SetBit(relativePageOffset, full) != full
}
