package datastore

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

const bufSize = 8192

const outFileName = "current-data"

var ErrNotFound = fmt.Errorf("record does not exist")

type hashIndex map[string]int64

type Segment struct {
	index   hashIndex
	outPath string
}

func (s *Segment) getValue(position int64) (string, error) {
	file, err := os.Open(s.outPath)
	if err != nil {
		return "", err
	}
	defer file.Close()

	reader := bufio.NewReader(file)
	_, err = reader.Discard(int(position))
	if err != nil {
		return "", err
	}

	value, err := readValue(reader)
	if err != nil {
		return "", err
	}

	return value, nil
}

type Db struct {
	out         *os.File
	outOffset   int64
	dir         string
	segmentSize int64
	totalNumber int
	segments    []*Segment
}

func NewDb(dir string, segmentSize int64) (*Db, error) {
	db := &Db{
		segments:    make([]*Segment, 0),
		dir:         dir,
		segmentSize: segmentSize,
	}

	err := db.createSegment()
	if err != nil {
		return nil, err
	}

	err = db.recover()
	if err != nil && err != io.EOF {
		return nil, err
	}

	return db, nil
}

func (db *Db) createSegment() error {
	outFile := fmt.Sprintf("%s%d", outFileName, db.totalNumber)
	outPath := filepath.Join(db.dir, outFile)
	db.totalNumber++

	f, err := os.OpenFile(outPath, os.O_APPEND|os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		return err
	}

	newSegment := &Segment{
		outPath: outPath,
		index:   make(hashIndex),
	}

	db.out.Close()
	db.out = f
	db.outOffset = 0

	db.segments = append(db.segments, newSegment)
	if len(db.segments) >= 3 {
		db.mergeSegments()
	}
	return nil
}

func (db *Db) mergeSegments() {
	go func() {
		outFile := fmt.Sprintf("%s%d", outFileName, db.totalNumber)
		outPath := filepath.Join(db.dir, outFile)
		db.totalNumber++

		newSegment := &Segment{
			outPath: outPath,
			index:   make(hashIndex),
		}
		var offset int64

		f, err := os.OpenFile(outPath, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0o600)
		if err != nil {
			return
		}
		defer f.Close()

		lastSegmentIndex := len(db.segments) - 2
		for i := 0; i <= lastSegmentIndex; i++ {
			s := db.segments[i]
			for key, index := range s.index {
				if i < lastSegmentIndex {
					duplicated := false
					for _, segment := range db.segments[i+1 : lastSegmentIndex+1] {
						if _, ok := segment.index[key]; ok {
							duplicated = true
							break
						}
					}
					if duplicated {
						continue
					}
				}
				value, _ := s.getValue(index)
				entry := entry{
					key:   key,
					value: value,
				}
				n, err := f.Write(entry.Encode())
				if err == nil {
					newSegment.index[key] = offset
					offset += int64(n)
				}
			}
		}

		db.segments = []*Segment{newSegment, db.segments[len(db.segments)-1]}
	}()
}

func (db *Db) recover() error {
	var err error
	var buf [bufSize]byte

	in := bufio.NewReaderSize(db.out, bufSize)
	for err == nil {
		var (
			header, data []byte
			n            int
		)
		header, err = in.Peek(bufSize)
		if err == io.EOF {
			if len(header) == 0 {
				return err
			}
		} else if err != nil {
			return err
		}
		size := binary.LittleEndian.Uint32(header)

		if size < bufSize {
			data = buf[:size]
		} else {
			data = make([]byte, size)
		}
		n, err = in.Read(data)

		if err == nil {
			var e entry
			e.Decode(data)
			db.segments[len(db.segments)-1].index[e.key] = db.outOffset
			db.outOffset += int64(n)
		}
	}

	return err
}

func (db *Db) getPosition(key string) (*Segment, int64, error) {
	for i := range db.segments {
		segment := db.segments[len(db.segments)-i-1]
		if pos, ok := segment.index[key]; ok {
			return segment, pos, nil
		}
	}

	return nil, 0, nil
}

func (db *Db) Get(key string) (string, error) {
	segment, position, err := db.getPosition(key)
	if err != nil {
		return "", err
	}

	return segment.getValue(position)
}

func (db *Db) Put(key, value string) error {
	entry := entry{
		key:   key,
		value: value,
	}
	size := int64(len(key) + len(value) + 12)

	stat, err := db.out.Stat()
	if err != nil {
		return err
	}

	if stat.Size()+size > db.segmentSize {
		err := db.createSegment()
		if err != nil {
			return err
		}
	}

	n, err := db.out.Write(entry.Encode())
	if err != nil {
		return err
	}

	db.segments[len(db.segments)-1].index[entry.key] = db.outOffset
	db.outOffset += int64(n)

	return nil
}
