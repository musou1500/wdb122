package storage

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBufferFetchPage(t *testing.T) {
	file, err := os.CreateTemp("", "test_buffer_fetch_page")
	require.NoError(t, err)
	dm, err := newDiskManager(file)
	require.NoError(t, err)

	bm, err := newBufferManager(dm, 1)
	require.NoError(t, err)
	desc, err := bm.CreatePage()
	pageId := desc.pageId
	require.NoError(t, err)
	b := [pageSize]byte{1, 2, 3, 4}
	copy(desc.page.bytes[:], b[:])
	desc.isDirty = true
	desc.Unpin()

	desc, err = bm.FetchPage(pageId)
	require.NoError(t, err)
	require.Equal(t, b, desc.page.bytes)
	desc.Unpin()
}

func TestBufferNoFreeBuffer(t *testing.T) {
	file, err := os.CreateTemp("", "test_buffer_no_free_buffer")
	require.NoError(t, err)
	dm, err := newDiskManager(file)
	require.NoError(t, err)

	bm, err := newBufferManager(dm, 1)
	require.NoError(t, err)

	bm.CreatePage()
	_, err = bm.CreatePage()
	require.Error(t, ErrNoFreeBuffer, err)
}
