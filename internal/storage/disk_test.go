package storage

import (
	"io"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDiskWriteReadPageData(t *testing.T) {
	file, err := os.CreateTemp("", "disk_test_write_page_data")
	require.NoError(t, err)
	diskManager, err := newDiskManager(file)
	require.NoError(t, err)

	pageId := diskManager.AllocatePage()
	require.Equal(t, PageId(0), pageId)
	bytes := [pageSize]byte{1, 2, 3, 4}
	diskManager.WritePageData(pageId, &Page{
		bytes,
	})
	page := Page{}
	diskManager.ReadPageData(pageId, &page)
	require.Equal(t, bytes, page.bytes)
}

func TestDiskEOF(t *testing.T) {
	file, err := os.CreateTemp("", "disk_test_write_page_data")
	require.NoError(t, err)
	diskManager, err := newDiskManager(file)
	require.NoError(t, err)

	require.Equal(t, PageId(0), diskManager.AllocatePage())

	pageId := PageId(1)
	_, err = diskManager.WritePageData(pageId, &Page{
		bytes: [pageSize]byte{1, 2, 3, 4},
	})
	require.Error(t, io.EOF, err)

	_, err = diskManager.ReadPageData(pageId, &Page{
		bytes: [pageSize]byte{},
	})
	require.Error(t, io.EOF, err)
}
