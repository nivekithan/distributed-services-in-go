package log

import (
	api "distributed-services-in-go/service/api/v1"
	"fmt"
	"os"
	"path"

	"google.golang.org/protobuf/proto"
)

type segment struct {
	store                  *store
	index                  *index
	baseOffset, nextOffset uint64
	config                 Config
}

func newSegment(dir string, baseOffset uint64, c Config) (*segment, error) {
	storeFile, err := os.OpenFile(
		path.Join(dir, fmt.Sprintf("%d%s", baseOffset, ".store")),
		os.O_RDWR|os.O_CREATE|os.O_APPEND,
		0644,
	)

	if err != nil {
		return nil, err
	}

	store, err := newStore(storeFile)

	if err != nil {
		return nil, err
	}

	indexFile, err := os.OpenFile(
		path.Join(dir, fmt.Sprintf("%d%s", baseOffset, ".index")),
		os.O_RDWR|os.O_CREATE|os.O_APPEND,
		0644,
	)

	if err != nil {
		return nil, err
	}

	index, err := newIndex(indexFile, c)

	if err != nil {
		return nil, err
	}

	offset, _, err := index.Read(-1)

	var nextOffset uint64
	if err != nil {
		nextOffset = baseOffset
	} else {
		nextOffset = baseOffset + uint64(offset) + 1
	}

	segment := segment{store: store, index: index, baseOffset: baseOffset, nextOffset: nextOffset, config: c}

	return &segment, nil
}

func (s *segment) Append(record *api.Record) (uint64, error) {
	curOffset := s.nextOffset
	record.Offset = curOffset

	encodedRecord, err := proto.Marshal(record)

	if err != nil {
		return 0, err
	}

	_, pos, err := s.store.Append(encodedRecord)

	if err != nil {
		return 0, nil
	}

	if err := s.index.Write(uint32(record.Offset-s.baseOffset), pos); err != nil {
		return 0, err
	}

	s.nextOffset++
	return curOffset, nil
}

func (s *segment) Read(offset uint64) (*api.Record, error) {
	_, pos, err := s.index.Read(int64(offset - s.baseOffset))

	if err != nil {
		return nil, err
	}

	encodedValue, err := s.store.Read(pos)

	if err != nil {
		return nil, err
	}

	record := &api.Record{}

	if err := proto.Unmarshal(encodedValue, record); err != nil {
		return nil, err
	}

	return record, nil
}

func (s *segment) IsMaxed() bool {
	return s.store.size >= s.config.Segment.MaxStoreBytes || s.index.size >= s.config.Segment.MaxIndexBytes
}

func (s *segment) Close() error {
	if err := s.index.Close(); err != nil {
		return err
	}

	if err := s.store.Close(); err != nil {
		return err
	}

	return nil

}

func (s *segment) Remove() error {
	if err := s.Close(); err != nil {
		return err
	}

	if err := os.Remove(s.index.Name()); err != nil {
		return err
	}

	if err := os.Remove(s.store.Name()); err != nil {
		return err
	}

	return nil
}

func nearestMultiple(j, k uint64) uint64 {
	return (j / k * k)
}
