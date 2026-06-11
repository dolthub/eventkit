package eventkit

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

func TestConcurrentWritersProduceValidFiles(t *testing.T) {
	dir := t.TempDir()
	fe, err := NewFileEmitter(dir)
	if err != nil {
		t.Fatal(err)
	}

	const goroutines = 16
	const perGoroutine = 8
	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(gid int) {
			defer wg.Done()
			for i := 0; i < perGoroutine; i++ {
				req := sampleReq()
				req.Events[0].ID = fmt.Sprintf("g%d-i%d", gid, i)
				if err := fe.Send(context.Background(), req); err != nil {
					t.Errorf("Send: %v", err)
					return
				}
			}
		}(g)
	}
	wg.Wait()

	target := &memEmitter{}
	if err := NewFileFlusher(dir, target).Flush(context.Background()); err != nil {
		t.Fatal(err)
	}

	target.mu.Lock()
	defer target.mu.Unlock()
	got := 0
	for _, r := range target.requests {
		got += len(r.Events)
	}
	if got != goroutines*perGoroutine {
		t.Fatalf("recovered %d events, want %d", got, goroutines*perGoroutine)
	}
}

func TestLeftoverTempFileIsIgnored(t *testing.T) {
	dir := t.TempDir()
	fe, err := NewFileEmitter(dir)
	if err != nil {
		t.Fatal(err)
	}

	tmpPath := filepath.Join(dir, ".write-leftover")
	if err := os.WriteFile(tmpPath, []byte("partial garbage"), 0o600); err != nil {
		t.Fatal(err)
	}

	if err := fe.Send(context.Background(), sampleReq()); err != nil {
		t.Fatal(err)
	}

	target := &memEmitter{}
	if err := NewFileFlusher(dir, target).Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(target.requests) != 1 {
		t.Fatalf("Send count = %d, want 1", len(target.requests))
	}

	if _, err := os.Stat(tmpPath); err != nil {
		t.Fatalf("leftover temp file should remain untouched: %v", err)
	}
}

func TestEmptyDirFlushIsNoop(t *testing.T) {
	dir := t.TempDir()
	target := &memEmitter{}
	if err := NewFileFlusher(dir, target).Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(target.requests) != 0 {
		t.Fatalf("expected no requests, got %d", len(target.requests))
	}
}

func TestMissingDirFlushIsNoop(t *testing.T) {
	target := &memEmitter{}
	if err := NewFileFlusher("/nonexistent/path/eventkit", target).Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestFlusherStressManyFiles(t *testing.T) {
	dir := t.TempDir()
	fe, err := NewFileEmitter(dir)
	if err != nil {
		t.Fatal(err)
	}
	const n = 100
	for i := 0; i < n; i++ {
		req := sampleReq()
		req.Events[0].ID = fmt.Sprintf("stress-%d", i)
		if err := fe.Send(context.Background(), req); err != nil {
			t.Fatal(err)
		}
	}

	target := &memEmitter{}
	if err := NewFileFlusher(dir, target).Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(target.requests) != n {
		t.Fatalf("Send count = %d, want %d", len(target.requests), n)
	}

	for _, e := range mustReadDir(t, dir) {
		if filepath.Ext(e.Name()) == DefaultFileExt {
			t.Fatalf("expected all files deleted, found %q", e.Name())
		}
	}
}

func TestFlusherValidMD5ButInvalidJSON(t *testing.T) {
	dir := t.TempDir()
	if _, err := NewFileEmitter(dir); err != nil {
		t.Fatal(err)
	}

	junk := []byte("not json at all")
	path := filepath.Join(dir, Filename(junk, DefaultFileExt))
	if err := os.WriteFile(path, junk, 0o600); err != nil {
		t.Fatal(err)
	}

	target := &memEmitter{}
	if err := NewFileFlusher(dir, target).Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(target.requests) != 0 {
		t.Fatalf("expected no requests, got %d", len(target.requests))
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("invalid-JSON file should remain on disk: %v", err)
	}
}
