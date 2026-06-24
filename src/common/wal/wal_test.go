package wal

import (
	"encoding/binary"
	"hash/crc32"
	"os"
	"reflect"
	"testing"
)

func collect(t *testing.T, dir, base string, gen uint64) [][]byte {
	t.Helper()
	var got [][]byte
	if err := Replay(dir, base, gen, func(p []byte) error {
		cp := make([]byte, len(p))
		copy(cp, p)
		got = append(got, cp)
		return nil
	}); err != nil {
		t.Fatalf("Replay: %v", err)
	}
	return got
}

func TestAppendReplayRoundTrip(t *testing.T) {
	dir := t.TempDir()
	l, err := Open(dir, "client_x", 0)
	if err != nil {
		t.Fatal(err)
	}
	recs := [][]byte{[]byte("a"), []byte("bb"), []byte("ccc"), {}, []byte("eeeee")}
	for _, r := range recs {
		if err := l.Append(r); err != nil {
			t.Fatal(err)
		}
	}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	got := collect(t, dir, "client_x", 0)
	want := [][]byte{[]byte("a"), []byte("bb"), []byte("ccc"), {}, []byte("eeeee")}
	if len(got) != len(want) {
		t.Fatalf("got %d records, want %d", len(got), len(want))
	}
	for i := range want {
		if !reflect.DeepEqual(got[i], want[i]) && !(len(got[i]) == 0 && len(want[i]) == 0) {
			t.Errorf("record %d: got %q want %q", i, got[i], want[i])
		}
	}
}

func TestAppendBatchSingleFlush(t *testing.T) {
	dir := t.TempDir()
	l, err := Open(dir, "c", 0)
	if err != nil {
		t.Fatal(err)
	}
	batch := [][]byte{[]byte("one"), []byte("two"), []byte("three")}
	if err := l.AppendBatch(batch); err != nil {
		t.Fatal(err)
	}
	l.Close()
	got := collect(t, dir, "c", 0)
	if len(got) != 3 {
		t.Fatalf("got %d, want 3", len(got))
	}
}

func TestReplayStopsAtTornTail(t *testing.T) {
	dir := t.TempDir()
	l, _ := Open(dir, "c", 0)
	l.Append([]byte("valid1"))
	l.Append([]byte("valid2"))
	l.Close()

	f, _ := os.OpenFile(segmentPath(dir, "c", 0), os.O_WRONLY|os.O_APPEND, 0o644)
	var hdr [recordHeaderSize]byte
	binary.LittleEndian.PutUint32(hdr[0:4], 100)
	binary.LittleEndian.PutUint32(hdr[4:8], 12345)
	f.Write(hdr[:])
	f.Write([]byte("abc"))
	f.Close()

	got := collect(t, dir, "c", 0)
	if len(got) != 2 || string(got[0]) != "valid1" || string(got[1]) != "valid2" {
		t.Fatalf("expected only the 2 intact records, got %d: %q", len(got), got)
	}
}

func TestReplayStopsAtBadCRC(t *testing.T) {
	dir := t.TempDir()
	l, _ := Open(dir, "c", 0)
	l.Append([]byte("good"))
	l.Close()

	f, _ := os.OpenFile(segmentPath(dir, "c", 0), os.O_WRONLY|os.O_APPEND, 0o644)
	payload := []byte("corrupt")
	var hdr [recordHeaderSize]byte
	binary.LittleEndian.PutUint32(hdr[0:4], uint32(len(payload)))
	binary.LittleEndian.PutUint32(hdr[4:8], crc32.ChecksumIEEE(payload)+1)
	f.Write(hdr[:])
	f.Write(payload)
	f.Close()

	got := collect(t, dir, "c", 0)
	if len(got) != 1 || string(got[0]) != "good" {
		t.Fatalf("expected only the intact record, got %d: %q", len(got), got)
	}
}

func TestRotateKeepsOnlyNewGen(t *testing.T) {
	dir := t.TempDir()
	l, _ := Open(dir, "c", 0)
	l.Append([]byte("gen0-a"))
	l.Append([]byte("gen0-b"))
	newGen, err := l.Rotate()
	if err != nil {
		t.Fatal(err)
	}
	if newGen != 1 {
		t.Fatalf("newGen=%d want 1", newGen)
	}
	l.Append([]byte("gen1-a"))
	l.Close()

	if _, err := os.Stat(segmentPath(dir, "c", 0)); !os.IsNotExist(err) {
		t.Fatalf("gen0 segment should be deleted")
	}
	got := collect(t, dir, "c", 1)
	if len(got) != 1 || string(got[0]) != "gen1-a" {
		t.Fatalf("gen1 should hold only its own records, got %q", got)
	}
}

func TestOpenDeletesStaleSegments(t *testing.T) {
	dir := t.TempDir()
	l0, _ := Open(dir, "c", 0)
	l0.Append([]byte("old"))
	l0.Close()
	l1, _ := Open(dir, "c", 1)
	l1.Close()

	if _, err := os.Stat(segmentPath(dir, "c", 0)); !os.IsNotExist(err) {
		t.Fatalf("stale gen0 segment should be deleted on Open(gen=1)")
	}
	if _, err := os.Stat(segmentPath(dir, "c", 1)); err != nil {
		t.Fatalf("active gen1 segment should exist: %v", err)
	}
}

func TestReplayMissingSegmentIsEmpty(t *testing.T) {
	dir := t.TempDir()
	got := collect(t, dir, "nope", 7)
	if len(got) != 0 {
		t.Fatalf("missing segment should replay nothing, got %d", len(got))
	}
}

func TestReopenContinuesAppend(t *testing.T) {
	dir := t.TempDir()
	l, _ := Open(dir, "c", 0)
	l.Append([]byte("first"))
	l.Close()

	l2, err := Open(dir, "c", 0)
	if err != nil {
		t.Fatal(err)
	}
	if l2.Size() <= 0 {
		t.Fatalf("reopened log should report existing size, got %d", l2.Size())
	}
	l2.Append([]byte("second"))
	l2.Close()

	got := collect(t, dir, "c", 0)
	if len(got) != 2 || string(got[0]) != "first" || string(got[1]) != "second" {
		t.Fatalf("reopen+append should preserve order, got %q", got)
	}
}

func TestRemoveDeletesAllSegments(t *testing.T) {
	dir := t.TempDir()
	l, _ := Open(dir, "c", 3)
	l.Append([]byte("x"))
	l.Close()
	if err := Remove(dir, "c"); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(segmentPath(dir, "c", 3)); !os.IsNotExist(err) {
		t.Fatalf("Remove should delete all segments")
	}
}
