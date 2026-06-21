package wal

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

const recordHeaderSize = 8

type Log struct {
	dir  string
	base string
	gen  uint64
	f    *os.File
	size int64
}

func segmentPath(dir, base string, gen uint64) string {
	return filepath.Join(dir, fmt.Sprintf("%s.%d.wal", base, gen))
}

func Open(dir, base string, gen uint64) (*Log, error) {
	if err := deleteStaleSegments(dir, base, gen); err != nil {
		return nil, err
	}
	path := segmentPath(dir, base, gen)
	created := false
	if _, err := os.Stat(path); errors.Is(err, os.ErrNotExist) {
		created = true
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, fmt.Errorf("wal open segment %s: %w", path, err)
	}
	if created {
		if err := fsyncDir(dir); err != nil {
			_ = f.Close()
			return nil, err
		}
	}
	info, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("wal stat segment %s: %w", path, err)
	}
	return &Log{dir: dir, base: base, gen: gen, f: f, size: info.Size()}, nil
}

func Replay(dir, base string, gen uint64, fn func(payload []byte) error) error {
	path := segmentPath(dir, base, gen)
	f, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("wal open for replay %s: %w", path, err)
	}
	defer f.Close()

	header := make([]byte, recordHeaderSize)
	for {
		if _, err := io.ReadFull(f, header); err != nil {
			return nil
		}
		n := binary.LittleEndian.Uint32(header[0:4])
		want := binary.LittleEndian.Uint32(header[4:8])
		payload := make([]byte, n)
		if _, err := io.ReadFull(f, payload); err != nil {
			return nil
		}
		if crc32.ChecksumIEEE(payload) != want {
			return nil
		}
		if err := fn(payload); err != nil {
			return fmt.Errorf("wal replay callback: %w", err)
		}
	}
}

func (l *Log) Append(payload []byte) error {
	return l.AppendBatch([][]byte{payload})
}

func (l *Log) AppendBatch(payloads [][]byte) error {
	if len(payloads) == 0 {
		return nil
	}
	buf := make([]byte, 0, 256)
	for _, p := range payloads {
		var hdr [recordHeaderSize]byte
		binary.LittleEndian.PutUint32(hdr[0:4], uint32(len(p)))
		binary.LittleEndian.PutUint32(hdr[4:8], crc32.ChecksumIEEE(p))
		buf = append(buf, hdr[:]...)
		buf = append(buf, p...)
	}
	if _, err := l.f.Write(buf); err != nil {
		return fmt.Errorf("wal append: %w", err)
	}
	if err := l.f.Sync(); err != nil {
		return fmt.Errorf("wal fsync: %w", err)
	}
	l.size += int64(len(buf))
	return nil
}

func (l *Log) Rotate() (uint64, error) {
	newGen := l.gen + 1
	newPath := segmentPath(l.dir, l.base, newGen)
	f, err := os.OpenFile(newPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return 0, fmt.Errorf("wal rotate create %s: %w", newPath, err)
	}
	if err := fsyncDir(l.dir); err != nil {
		_ = f.Close()
		return 0, err
	}
	oldPath := segmentPath(l.dir, l.base, l.gen)
	_ = l.f.Close()
	if err := os.Remove(oldPath); err != nil && !errors.Is(err, os.ErrNotExist) {
		_ = f.Close()
		return 0, fmt.Errorf("wal rotate remove old %s: %w", oldPath, err)
	}
	l.f = f
	l.gen = newGen
	l.size = 0
	return newGen, nil
}

func (l *Log) Size() int64 { return l.size }

func (l *Log) Gen() uint64 { return l.gen }

func (l *Log) Close() error {
	if l.f == nil {
		return nil
	}
	err := l.f.Close()
	l.f = nil
	return err
}

func Remove(dir, base string) error {
	segs, err := filepath.Glob(filepath.Join(dir, base+".*.wal"))
	if err != nil {
		return err
	}
	for _, s := range segs {
		if !isSegmentOf(filepath.Base(s), base) {
			continue
		}
		if err := os.Remove(s); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
	}
	return nil
}

func deleteStaleSegments(dir, base string, keep uint64) error {
	segs, err := filepath.Glob(filepath.Join(dir, base+".*.wal"))
	if err != nil {
		return err
	}
	for _, s := range segs {
		name := filepath.Base(s)
		gen, ok := segmentGen(name, base)
		if !ok || gen == keep {
			continue
		}
		if err := os.Remove(s); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
	}
	return nil
}

func segmentGen(name, base string) (uint64, bool) {
	prefix := base + "."
	if !strings.HasPrefix(name, prefix) || !strings.HasSuffix(name, ".wal") {
		return 0, false
	}
	mid := strings.TrimSuffix(strings.TrimPrefix(name, prefix), ".wal")
	gen, err := strconv.ParseUint(mid, 10, 64)
	if err != nil {
		return 0, false
	}
	return gen, true
}

func isSegmentOf(name, base string) bool {
	_, ok := segmentGen(name, base)
	return ok
}

func fsyncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return fmt.Errorf("wal open dir for fsync %s: %w", dir, err)
	}
	defer d.Close()
	if err := d.Sync(); err != nil {
		return fmt.Errorf("wal fsync dir %s: %w", dir, err)
	}
	return nil
}
