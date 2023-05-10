package storage

import (
	"fmt"
)

type BufferId int

var ErrNoFreeBuffer = fmt.Errorf("no free buffer available")

type BufferDescriptor struct {
	pageId     PageId
	refCount   uint64
	usageCount uint64
	isDirty    bool
	page       Page
}

type BufferManager struct {
	diskManager       *DiskManager
	bufferTable       map[PageId]BufferId
	bufferDescriptors []BufferDescriptor
}

func newBufferManager(
	diskManager *DiskManager,
	poolSize int,
) (*BufferManager, error) {
	return &BufferManager{
		diskManager:       diskManager,
		bufferDescriptors: make([]BufferDescriptor, poolSize),
		bufferTable:       make(map[PageId]BufferId),
	}, nil
}

func (bm *BufferManager) poolSize() int {
	return len(bm.bufferDescriptors)
}

func (bd *BufferDescriptor) pin() {
	bd.usageCount++
	bd.refCount++
}

func (bd *BufferDescriptor) Unpin() {
	bd.refCount--
}

func (bm *BufferManager) evict() (BufferId, bool) {
	consecutivePins := 0
	for nextVictimId := BufferId(0); consecutivePins < bm.poolSize(); nextVictimId = BufferId(bm.incrementId(nextVictimId)) {
		desc := bm.bufferDescriptors[nextVictimId]
		if desc.refCount > 0 {
			consecutivePins++
			continue
		}

		consecutivePins = 0
		if desc.usageCount == 0 {
			return nextVictimId, true
		} else {
			desc.usageCount--
		}
	}
	return 0, false
}

func (bm *BufferManager) incrementId(bufferId BufferId) int {
	return (int(bufferId) + 1) % bm.poolSize()
}

func (bm *BufferManager) FetchPage(pageId PageId) (*BufferDescriptor, error) {
	if bufferId, ok := bm.bufferTable[pageId]; ok {
		desc := bm.bufferDescriptors[bufferId]
		desc.pin()
		return &desc, nil
	}
	bufferId, ok := bm.evict()
	if !ok {
		return nil, ErrNoFreeBuffer
	}
	desc := &bm.bufferDescriptors[bufferId]
	evictPageId := desc.pageId
	if desc.isDirty {
		bm.diskManager.WritePageData(pageId, &desc.page)
	}
	desc.pageId = pageId
	desc.isDirty = false
	desc.refCount = 0
	desc.usageCount = 0
	_, err := bm.diskManager.ReadPageData(pageId, &desc.page)
	if err != nil {
		return nil, err
	}
	desc.pin()
	delete(bm.bufferTable, evictPageId)
	bm.bufferTable[pageId] = bufferId
	return desc, nil
}

func (bm *BufferManager) CreatePage() (*BufferDescriptor, error) {
	bufferId, ok := bm.evict()
	if !ok {
		return nil, ErrNoFreeBuffer
	}

	desc := &bm.bufferDescriptors[bufferId]
	if desc.isDirty {
		bm.diskManager.WritePageData(desc.pageId, &desc.page)
	}
	evictPageId := desc.pageId
	desc.pageId = bm.diskManager.AllocatePage()
	desc.isDirty = false
	desc.usageCount = 0
	desc.refCount = 0
	desc.page.bytes = [pageSize]byte{}
	desc.pin()
	delete(bm.bufferTable, evictPageId)
	bm.bufferTable[desc.pageId] = bufferId
	return desc, nil
}
