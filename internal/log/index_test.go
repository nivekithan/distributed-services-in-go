package log

import (
	"io"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIndex(t *testing.T) {
	f, err := os.CreateTemp("", "index_test")

	require.NoError(t, err)

	defer os.Remove(f.Name())

	c := Config{}

	c.Segment.MaxIndexBytes = 1024

	index, err := newIndex(f, c)

	require.NoError(t, err)

	_, _, err = index.Read(-1)

	require.Error(t, err)
	require.Equal(t, io.EOF, err)
	require.Equal(t, f.Name(), index.Name())

	entries := []struct {
		Offset uint32
		Pos    uint64
	}{
		{Offset: 0, Pos: 0},
		{Offset: 1, Pos: 10},
	}

	for _, want := range entries {
		err := index.Write(want.Offset, want.Pos)

		require.NoError(t, err)

		offset, pos, err := index.Read(int64(want.Offset))

		require.NoError(t, err)
		require.Equal(t, want.Pos, pos)
		require.Equal(t, want.Offset, offset)
	}

	// Reading offset greater than size should return err
	_, _, err = index.Read(int64(len(entries)))

	require.Error(t, err)
	require.Equal(t, io.EOF, err)

	// Should be able to close the index without any error
	err = index.Close()
	require.NoError(t, err)

	// Index should build its state from the existing file
	f, _ = os.OpenFile(f.Name(), os.O_RDWR, 0600)

	index, err = newIndex(f, c)

	require.NoError(t, err)

	offset, pos, err := index.Read(-1)

	require.NoError(t, err)

	require.Equal(t, uint32(1), offset)
	require.Equal(t, uint64(10), pos)
}
