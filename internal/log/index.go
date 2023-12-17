package log

import (
	"io"
	"log"
	"os"

	"github.com/edsrzf/mmap-go"
)

type index struct {
	file *os.File
	mmap mmap.MMap
	size uint64
}

var (
	offsetWidth uint64 = 4
	posWidth    uint64 = 8
	entierWidth        = offsetWidth + posWidth
)

func newIndex(file *os.File, config Config) (*index, error) {

	fileStat, err := os.Stat(file.Name())

	if err != nil {
		log.Printf("os.Stat %v", err.Error())
		return nil, err
	}

	size := uint64(fileStat.Size())

	if err := os.Truncate(file.Name(), int64(config.Segment.MaxIndexBytes)); err != nil {
		log.Printf("os.Truncate %v", err.Error())
		return nil, err
	}

	mmapBytes, err := mmap.Map(file, mmap.RDWR, 0)

	if err != nil {
		log.Printf("mmap.Map %v", err.Error())
		return nil, err
	}

	return &index{file: file, mmap: mmapBytes, size: size}, nil
}

func (i *index) Close() error {
	if err := i.mmap.Unmap(); err != nil {
		return err
	}

	if err := i.file.Sync(); err != nil {
		return err
	}

	if err := i.file.Truncate(int64(i.size)); err != nil {
		return err
	}

	return i.file.Close()
}

func (i *index) Read(offset int64) (uint32, uint64, error) {
	if i.size == 0 {
		return 0, 0, io.EOF
	}

	var out uint64

	if offset == -1 {
		out = uint64(i.size/entierWidth - 1)
	} else {
		out = uint64(offset)
	}

	if isOffsetOutOfRange := i.size < ((out + 1) * entierWidth); isOffsetOutOfRange {
		return 0, 0, io.EOF
	}

	offsetStartsAt := out * entierWidth
	offsetEndsAt := offsetStartsAt + offsetWidth

	posStartsAt := offsetEndsAt
	posEndsAt := posStartsAt + posWidth

	outOffset := enc.Uint32(i.mmap[offsetStartsAt:offsetEndsAt])
	pos := enc.Uint64(i.mmap[posStartsAt:posEndsAt])

	return outOffset, pos, nil
}

func (i *index) Write(offset uint32, pos uint64) error {
	if isSpaceNotAvaliable := uint64(len(i.mmap)) < (i.size)+(entierWidth); isSpaceNotAvaliable {
		return io.EOF
	}

	offsetStartsAt := i.size
	offsetEndsAt := i.size + offsetWidth

	posStartsAt := offsetEndsAt
	posEndsAt := posStartsAt + posWidth

	enc.PutUint32(i.mmap[offsetStartsAt:offsetEndsAt], offset)
	enc.PutUint64(i.mmap[posStartsAt:posEndsAt], pos)

	i.size += entierWidth
	return nil
}

func (i *index) Name() string {
	return i.file.Name()
}
