package log

import (
	"bufio"
	"encoding/binary"
	"os"
	"sync"
)

var (
	enc = binary.BigEndian
)

const (
	lenWidth = 8 // 64 bits is 8 bytes
)

type store struct {
	mu   sync.Mutex
	File *os.File
	buf  *bufio.Writer
	size uint64
}

func newStore(file *os.File) (*store, error) {
	fi, err := os.Stat(file.Name())

	if err != nil {
		return nil, err
	}

	size := uint64(fi.Size())

	return &store{File: file, buf: bufio.NewWriter(file), size: size}, nil
}

func (s *store) Append(value []byte) (written uint64, pos uint64, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	size_of_data := uint64(len(value))

	if err := binary.Write(s.buf, enc, size_of_data); err != nil {
		return 0, 0, err
	}

	written_size, err := s.buf.Write(value)

	if err != nil {
		return 0, 0, err
	}

	total_written_size := +uint64(written_size) + lenWidth
	pos = s.size
	s.size += total_written_size

	return total_written_size, pos, nil
}

func (s *store) Read(pos uint64) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.buf.Flush(); err != nil {
		return nil, err
	}

	size := make([]byte, lenWidth)

	if _, err := s.File.ReadAt(size, int64(pos)); err != nil {
		return nil, err
	}

	data := make([]byte, enc.Uint64(size))

	if _, err := s.File.ReadAt(data, int64(pos+lenWidth)); err != nil {
		return nil, err
	}

	return data, nil
}

func (s *store) ReadAt(data []byte, offset int64) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.buf.Flush(); err != nil {
		return 0, err
	}

	return s.File.ReadAt(data, offset)
}

func (s *store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.buf.Flush(); err != nil {
		return err
	}

	return s.File.Close()
}
