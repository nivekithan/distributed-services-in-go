package log

import (
	api "distributed-services-in-go/service/api/v1"
	"io"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSegment(t *testing.T) {
	dir, err := os.MkdirTemp("", "segment-test")

	require.NoError(t, err)

	defer os.RemoveAll(dir)

	want := &api.Record{Value: []byte("Hello World!")}

	c := Config{}

	c.Segment.MaxStoreBytes = 1024
	c.Segment.MaxIndexBytes = entierWidth * 3

	s, err := newSegment(dir, 16, c)

	require.NoError(t, err)

	require.Equal(t, uint64(16), s.baseOffset)

	require.False(t, s.IsMaxed())

	for i := uint64(0); i < 3; i++ {
		off, err := s.Append(want)
		require.NoError(t, err)

		require.Equal(t, 16+i, off)

		got, err := s.Read(off)

		require.NoError(t, err)

		require.Equal(t, want.Value, got.Value)
	}

	_, err = s.Append(want)

	require.Equal(t, io.EOF, err)

	// Maxed Index

	require.True(t, s.IsMaxed())

	c.Segment.MaxStoreBytes = uint64(len(want.Value) * 3)
	c.Segment.MaxIndexBytes = 1024

	s, err = newSegment(dir, 16, c)

	require.NoError(t, err)

	require.True(t, s.IsMaxed())

	err = s.Remove()

	require.NoError(t, err)

	s, err = newSegment(dir, 16, c)

	require.NoError(t, err)
	require.False(t, s.IsMaxed())
}
