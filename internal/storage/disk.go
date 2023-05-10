package storage

import "os"

const (
	pageSize = 4096
)

type Page struct {
	bytes [pageSize]byte
}

type DiskManager struct {
	file       *os.File
	nextPageId uint
}

type PageId int

func newDiskManager(file *os.File) (*DiskManager, error) {
	finfo, err := file.Stat()
	if err != nil {
		return nil, err
	}

	return &DiskManager{
		file:       file,
		nextPageId: uint(finfo.Size() / pageSize),
	}, nil
}

func (dm *DiskManager) AllocatePage() PageId {
	pageId := dm.nextPageId
	dm.nextPageId++
	return PageId(pageId)
}

func (dm *DiskManager) WritePageData(pageId PageId, page *Page) (int, error) {
	offset := int64(pageId) * pageSize
	return dm.file.WriteAt(page.bytes[:], offset)
}

func (dm *DiskManager) ReadPageData(pageId PageId, page *Page) (int, error) {
	offset := int64(pageId) * pageSize
	return dm.file.ReadAt(page.bytes[:], offset)
}
