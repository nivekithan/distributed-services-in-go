package server

import (
	"fmt"
	"sync"
)

type Record struct {
	Value  []byte `json:"value"`
	Offset int    `json:"offset"`
}

type Log struct {
	mu      sync.Mutex
	records []Record
}

func NewLog() *Log {
	return &Log{}
}

func (log *Log) Append(value []byte) (int, error) {
	log.mu.Lock()

	defer log.mu.Unlock()

	record := Record{Value: value, Offset: len(log.records)}
	log.records = append(log.records, record)

	return record.Offset, nil
}

var ErrOffsetNotFound = fmt.Errorf("Offset not found")

func (log *Log) Read(offset int) (Record, error) {
	log.mu.Lock()
	defer log.mu.Unlock()

	if offset >= len(log.records) {
		return Record{}, ErrOffsetNotFound
	}

	return log.records[offset], nil
}
