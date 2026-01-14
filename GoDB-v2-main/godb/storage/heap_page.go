package storage

import (
	"encoding/binary"

	"mit.edu/dsg/godb/common"
)

// HeapPage Layout:
// LSN (8) | RowSize (2) | NumSlots (2) |  NumUsed (2) | Padding (2) | allocation Bitmap | deleted Bitmap | rows
type HeapPage struct {
	*PageFrame

	// <silentstrip lab1|lab2|lab3|lab4>
	// Computed on creation for performance in repeated access
	allocationBitmap Bitmap
	deletedBitmap    Bitmap
	rowDataStart     int
	// </silentstrip>
}

// <silentstrip lab1|lab2|lab3|lab4>
const (
	heapPageOffsetRowSize  = pageOffsetLSN + 8
	heapPageOffsetNumSlots = heapPageOffsetRowSize + 2
	heapPageOffsetNumUsed  = heapPageOffsetNumSlots + 2
)
const heapPageHeaderSize = heapPageOffsetNumUsed + 4

// </silentstrip>

func (hp HeapPage) NumUsed() int {
	// <silentstrip lab1|lab2|lab3|lab4>
	return int(binary.LittleEndian.Uint16(hp.Bytes[heapPageOffsetNumUsed:]))
	// </silentstrip>
	// <insert lab1|lab2|lab3|lab4>
	// panic("unimplemented")
	// </insert>
}

func (hp HeapPage) setNumUsed(numUsed int) {
	// <silentstrip lab1|lab2|lab3|lab4>
	binary.LittleEndian.PutUint16(hp.Bytes[heapPageOffsetNumUsed:], uint16(numUsed))
	// </silentstrip>
	// <insert lab1|lab2|lab3|lab4>
	// panic("unimplemented")
	// </insert>
}

func (hp HeapPage) NumSlots() int {
	// <silentstrip lab1|lab2|lab3|lab4>
	return int(binary.LittleEndian.Uint16(hp.Bytes[heapPageOffsetNumSlots:]))
	// </silentstrip>
	// <insert lab1|lab2|lab3|lab4>
	// panic("unimplemented")
	// </insert>
}

func (hp HeapPage) RowSize() int {
	// <silentstrip lab1|lab2|lab3|lab4>
	return int(binary.LittleEndian.Uint16(hp.Bytes[heapPageOffsetRowSize:]))
	// </silentstrip>
	// <insert lab1|lab2|lab3|lab4>
	// panic("unimplemented")
	// </insert>
}

func InitializeHeapPage(desc *RawTupleDesc, frame *PageFrame) {
	// <silentstrip lab1|lab2|lab3|lab4>
	rowSize := desc.BytesPerTuple()
	common.Assert(common.AlignedTo8(rowSize), "tuple numBits %d should be aligned to 8", rowSize)
	// Utilization is full per 64 rows with aligned bitmaps
	blockSize := (64 * rowSize) + 16
	available := common.PageSize - heapPageHeaderSize
	fullBlocks, remainder := available/blockSize, available%blockSize
	numSlots := fullBlocks * 64
	if remainder > 16 {
		numSlots += (remainder - 16) / rowSize
	}
	binary.LittleEndian.PutUint16(frame.Bytes[heapPageOffsetRowSize:], uint16(rowSize))
	binary.LittleEndian.PutUint16(frame.Bytes[heapPageOffsetNumSlots:], uint16(numSlots))
	// </silentstrip>
	// <insert lab1|lab2|lab3|lab4>
	// panic("unimplemented")
	// </insert>
}

func (frame *PageFrame) AsHeapPage() HeapPage {
	// <silentstrip lab1|lab2|lab3|lab4>
	result := HeapPage{
		PageFrame: frame,
	}
	numSlots := result.NumSlots()
	common.Assert(result.RowSize() > 0 && numSlots > 0, "uninitialized heap page")

	result.allocationBitmap = AsBitmap(result.Bytes[heapPageHeaderSize:], numSlots)
	bitmapSize := common.Align8((numSlots + 7) / 8)
	result.deletedBitmap = AsBitmap(frame.Bytes[heapPageHeaderSize+bitmapSize:], numSlots)
	result.rowDataStart = heapPageHeaderSize + 2*bitmapSize
	return result
	// </silentstrip>
	// <insert lab1|lab2|lab3|lab4>
	// panic("unimplemented")
	// </insert>
}

func (hp HeapPage) FindFreeSlot() int {
	// <silentstrip lab1|lab2|lab3|lab4>
	numUsed := hp.NumUsed()
	if numUsed == hp.NumSlots() {
		return -1
	}

	return hp.allocationBitmap.FindFirstZero(numUsed)
	// </silentstrip>
	// <insert lab1|lab2|lab3|lab4>
	// panic("unimplemented")
	// </insert>
}

// IsAllocated checks the allocation bitmap to see if a slot is valid.
func (hp HeapPage) IsAllocated(rid common.RecordID) bool {
	// <silentstrip lab1|lab2|lab3|lab4>
	slot := int(rid.Slot)
	// We do not assert bounds here to allow safe iteration
	if slot < 0 || slot >= hp.NumSlots() {
		return false
	}
	return hp.allocationBitmap.LoadBit(slot)
	// </silentstrip>
	// <insert lab1|lab2|lab3|lab4>
	// panic("unimplemented")
	// </insert>
}

func (hp HeapPage) MarkAllocated(rid common.RecordID, allocated bool) {
	// <silentstrip lab1|lab2|lab3|lab4>
	slot := int(rid.Slot)
	common.Assert(slot >= 0 && slot < hp.NumSlots(), "slot out of bounds")
	hp.allocationBitmap.SetBit(slot, allocated)
	if allocated {
		hp.setNumUsed(hp.NumUsed() + 1)
	} else {
		hp.deletedBitmap.SetBit(slot, false)
		hp.setNumUsed(hp.NumUsed() - 1)
	}
	// </silentstrip>
	// <insert lab1|lab2|lab3|lab4>
	// panic("unimplemented")
	// </insert>
}

func (hp HeapPage) IsDeleted(rid common.RecordID) bool {
	// <silentstrip lab1|lab2|lab3|lab4>
	slot := int(rid.Slot)
	common.Assert(slot >= 0 && slot < hp.NumSlots(), "slot out of bounds")
	return hp.deletedBitmap.LoadBit(slot)
	// </silentstrip>
	// <insert lab1|lab2|lab3|lab4>
	// panic("unimplemented")
	// </insert>
}

func (hp HeapPage) MarkDeleted(rid common.RecordID, deleted bool) {
	// <silentstrip lab1|lab2|lab3|lab4>
	slot := int(rid.Slot)
	common.Assert(slot >= 0 && slot < hp.NumSlots(), "slot out of bounds")
	common.Assert(hp.allocationBitmap.LoadBit(slot), "slot not allocated")
	hp.deletedBitmap.SetBit(slot, deleted)
	// </silentstrip>
	// <insert lab1|lab2|lab3|lab4>
	// panic("unimplemented")
	// </insert>
}

func (hp HeapPage) AccessTuple(rid common.RecordID) RawTuple {
	// <silentstrip lab1|lab2|lab3|lab4>
	slot := int(rid.Slot)
	common.Assert(slot >= 0 && slot < hp.NumSlots(), "slot out of bounds")
	common.Assert(hp.allocationBitmap.LoadBit(slot), "slot not allocated")
	return hp.Bytes[hp.rowDataStart+slot*hp.RowSize() : hp.rowDataStart+(slot+1)*hp.RowSize()]
	// </silentstrip>
	// <insert lab1|lab2|lab3|lab4>
	// panic("unimplemented")
	// </insert>
}
