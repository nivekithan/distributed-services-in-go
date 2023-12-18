package log

import (
	"fmt"
	"io"
	"os"
	"path"
	"slices"
	"strconv"
	"strings"
	"sync"

	api "distributed-services-in-go/service/api/v1"
)

type Log struct {
	mu sync.RWMutex

	Dir           string
	Config        Config
	activeSegment *segment
	segments      []*segment
}

func NewLog(dir string, c Config) (*Log, error) {
	if c.Segment.MaxStoreBytes == 0 {
		c.Segment.MaxStoreBytes = 1024
	}

	if c.Segment.MaxIndexBytes == 0 {
		c.Segment.MaxIndexBytes = 1024
	}

	log := &Log{Dir: dir, Config: c}

	if err := log.setup(); err != nil {
		return nil, err
	}

	return log, nil
}

func (l *Log) setup() error {

	files, err := os.ReadDir(l.Dir)

	if err != nil {
		return err
	}

	baseOffsets := []uint64{}

	for _, file := range files {
		offstr := strings.TrimSuffix(file.Name(), path.Ext(file.Name()))

		off, err := strconv.ParseUint(offstr, 10, 0)

		if err != nil {
			return err
		}

		baseOffsets = append(baseOffsets, off)
	}

	slices.Sort(baseOffsets)

	for i := 0; i < len(baseOffsets); i += 2 {
		err := l.newSegment(baseOffsets[i])

		if err != nil {
			return err
		}

	}

	if l.segments == nil {
		err := l.newSegment(l.Config.Segment.InitialOffset)

		if err != nil {
			return err
		}
	}

	return nil
}
func (l *Log) newSegment(baseOffset uint64) error {
	s, err := newSegment(l.Dir, baseOffset, l.Config)

	if err != nil {
		return err
	}

	l.segments = append(l.segments, s)
	l.activeSegment = s

	return nil
}

func (l *Log) Append(record *api.Record) (uint64, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	off, err := l.activeSegment.Append(record)

	if err != nil {
		return 0, err
	}

	if l.activeSegment.IsMaxed() {
		if err := l.newSegment(off + 1); err != nil {
			return 0, err
		}

	}

	return off, err
}

func (l *Log) Read(off uint64) (*api.Record, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()

	var segment *segment
	for _, s := range l.segments {
		if s.baseOffset <= off && s.nextOffset > off {
			segment = s
			break
		}
	}

	if segment == nil {
		return nil, fmt.Errorf("Offset out of range: %d", off)
	}

	return segment.Read(off)
}

func (l *Log) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()

	for _, segment := range l.segments {
		if err := segment.Close(); err != nil {
			return err
		}
	}

	return nil
}

func (l *Log) Remove() error {
	if err := l.Close(); err != nil {
		return err
	}

	return os.RemoveAll(l.Dir)
}

func (l *Log) Reset() error {
	if err := l.Remove(); err != nil {
		return err
	}

	return l.setup()
}

func (l *Log) LowestOffset() (uint64, error) {
	l.mu.RLock()

	defer l.mu.RUnlock()

	return l.segments[0].baseOffset, nil
}

func (l *Log) HighestOffset() (uint64, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()

	off := l.segments[len(l.segments)-1].nextOffset

	if off == 0 {
		return 0, nil
	}

	return off - 1, nil
}

func (l *Log) Truncate(lowest uint64) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	segments := []*segment{}

	for _, s := range l.segments {
		if s.nextOffset-1 <= lowest {
			if err := s.Remove(); err != nil {
				return err
			}

			continue
		}
		segments = append(segments, s)
	}

	l.segments = segments
	return nil
}

func (l *Log) Reader() io.Reader {
	l.mu.RLock()
	defer l.mu.RUnlock()

	readers := make([]io.Reader, len(l.segments))

	for i, segment := range l.segments {
		readers[i] = &orignReader{store: segment.store, off: 0}
	}

	return io.MultiReader(readers...)
}

type orignReader struct {
	store *store
	off   int64
}

func (o *orignReader) Read(p []byte) (int, error) {
	n, err := o.store.ReadAt(p, o.off)

	o.off += int64(n)

	return n, err
}
