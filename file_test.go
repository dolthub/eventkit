package eventkit

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func sampleReq() *LogEventsRequest {
	return &LogEventsRequest{
		DistinctID: "did",
		AppName:    "app",
		AppVersion: "1.0",
		Platform:   "linux",
		Events: []EventRecord{{
			ID:        "evt-1",
			Name:      "command.test",
			StartTime: time.Unix(1000, 0).UTC(),
			EndTime:   time.Unix(1002, 0).UTC(),
		}},
	}
}

func TestFilenameRoundTrip(t *testing.T) {
	data := []byte(`{"hello":"world"}`)
	name := Filename(data, DefaultFileExt)
	if !strings.HasSuffix(name, DefaultFileExt) {
		t.Fatalf("name = %q", name)
	}
	if !CheckFilenameMD5(data, filepath.Join("/some/dir", name), DefaultFileExt) {
		t.Fatal("CheckFilenameMD5 false on matching data")
	}
	if CheckFilenameMD5([]byte("different"), filepath.Join("/some/dir", name), DefaultFileExt) {
		t.Fatal("CheckFilenameMD5 true on mismatched data")
	}
	if CheckFilenameMD5(data, "/some/dir/other.txt", DefaultFileExt) {
		t.Fatal("CheckFilenameMD5 true for wrong extension")
	}
}

func TestFileEmitterWritesValidFile(t *testing.T) {
	dir := t.TempDir()
	fe, err := NewFileEmitter(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := fe.Send(context.Background(), sampleReq()); err != nil {
		t.Fatal(err)
	}

	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	var found bool
	for _, e := range entries {
		if filepath.Ext(e.Name()) == DefaultFileExt {
			found = true
			data, err := os.ReadFile(filepath.Join(dir, e.Name()))
			if err != nil {
				t.Fatal(err)
			}
			if !CheckFilenameMD5(data, e.Name(), DefaultFileExt) {
				t.Fatalf("MD5 mismatch for %q", e.Name())
			}
		}
	}
	if !found {
		t.Fatal("no .evtq file written")
	}
}

func TestFileEmitterEmptyBatchIsNoop(t *testing.T) {
	dir := t.TempDir()
	fe, err := NewFileEmitter(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := fe.Send(context.Background(), &LogEventsRequest{}); err != nil {
		t.Fatal(err)
	}
	entries, _ := os.ReadDir(dir)
	if len(entries) != 0 {
		t.Fatalf("expected empty dir, got %d entries", len(entries))
	}
}

func TestFileEmitterFlusherRoundTrip(t *testing.T) {
	dir := t.TempDir()
	fe, err := NewFileEmitter(dir)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		req := sampleReq()
		req.Events[0].ID = string(rune('a' + i))
		if err := fe.Send(context.Background(), req); err != nil {
			t.Fatal(err)
		}
	}

	target := &memEmitter{}
	flusher := NewFileFlusher(dir, target)
	if err := flusher.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(target.requests) != 3 {
		t.Fatalf("requests = %d, want 3", len(target.requests))
	}

	entries, _ := os.ReadDir(dir)
	for _, e := range entries {
		if filepath.Ext(e.Name()) == DefaultFileExt {
			t.Fatalf("expected .evtq files deleted, found %q", e.Name())
		}
	}
}

func TestFileFlusherSkipsCorrupt(t *testing.T) {
	dir := t.TempDir()
	fe, err := NewFileEmitter(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := fe.Send(context.Background(), sampleReq()); err != nil {
		t.Fatal(err)
	}

	entries, _ := os.ReadDir(dir)
	var path string
	for _, e := range entries {
		if filepath.Ext(e.Name()) == DefaultFileExt {
			path = filepath.Join(dir, e.Name())
		}
	}
	if err := os.WriteFile(path, []byte("garbage"), 0o644); err != nil {
		t.Fatal(err)
	}

	target := &memEmitter{}
	flusher := NewFileFlusher(dir, target)
	if err := flusher.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(target.requests) != 0 {
		t.Fatalf("expected zero requests, got %d", len(target.requests))
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("corrupt file should remain on disk: %v", err)
	}
}

func TestCollectorWritesBatchToDisk(t *testing.T) {
	dir := t.TempDir()
	fe, err := NewFileEmitter(dir)
	if err != nil {
		t.Fatal(err)
	}
	c := NewCollector(fe,
		WithMaxBatch(3),
		WithDistinctID("did"),
		WithAppName("test"),
		WithAppVersion("9.9.9"),
		WithPlatform("linux"),
	)
	for i := 0; i < 3; i++ {
		c.CloseEventAndAdd(NewEvent("cmd"))
	}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		files := 0
		for _, e := range mustReadDir(t, dir) {
			if filepath.Ext(e.Name()) == DefaultFileExt {
				files++
			}
		}
		if files >= 1 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	for i := 0; i < 2; i++ {
		c.CloseEventAndAdd(NewEvent("cmd"))
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := c.Close(ctx); err != nil {
		t.Fatal(err)
	}

	files := 0
	for _, e := range mustReadDir(t, dir) {
		if filepath.Ext(e.Name()) == DefaultFileExt {
			files++
		}
	}
	if files < 2 {
		t.Fatalf("expected >= 2 .evtq files, got %d", files)
	}

	target := &memEmitter{}
	if err := NewFileFlusher(dir, target).Flush(context.Background()); err != nil {
		t.Fatal(err)
	}

	target.mu.Lock()
	defer target.mu.Unlock()
	total := 0
	for _, r := range target.requests {
		if r.DistinctID != "did" || r.AppName != "test" || r.AppVersion != "9.9.9" || r.Platform != "linux" {
			t.Fatalf("envelope = %+v", r)
		}
		total += len(r.Events)
	}
	if total != 5 {
		t.Fatalf("recovered %d events, want 5", total)
	}
}

func mustReadDir(t *testing.T, dir string) []os.DirEntry {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	return entries
}

func TestFileFlusherSecondInstanceExitsCleanly(t *testing.T) {
	dir := t.TempDir()
	fe, err := NewFileEmitter(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := fe.Send(context.Background(), sampleReq()); err != nil {
		t.Fatal(err)
	}

	blocker := &blockingEmitter{ch: make(chan struct{}), entered: make(chan struct{})}
	first := NewFileFlusher(dir, blocker)
	done := make(chan error, 1)
	go func() { done <- first.Flush(context.Background()) }()

	select {
	case <-blocker.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("first flusher never entered Send")
	}

	second := NewFileFlusher(dir, &memEmitter{})
	if err := second.Flush(context.Background()); err != nil {
		t.Fatalf("second flush should be nil, got %v", err)
	}

	close(blocker.ch)
	if err := <-done; err != nil {
		t.Fatalf("first flush: %v", err)
	}
}

type blockingEmitter struct {
	entered    chan struct{}
	enteredMu  sync.Mutex
	enteredHit bool
	ch         chan struct{}
}

func (b *blockingEmitter) Send(ctx context.Context, _ *LogEventsRequest) error {
	b.enteredMu.Lock()
	if !b.enteredHit {
		b.enteredHit = true
		close(b.entered)
	}
	b.enteredMu.Unlock()
	select {
	case <-b.ch:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
