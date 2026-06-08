package eventkit

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"

	"github.com/dolthub/fslock"
)

type Drainable interface {
	Drain(ctx context.Context) (map[string]error, error)
}

type FileFlusher struct {
	dir          string
	ext          string
	lockFilename string
	target       Emitter
}

type FileFlusherOption func(*FileFlusher)

func WithFlusherFileExtension(ext string) FileFlusherOption {
	return func(f *FileFlusher) { f.ext = ext }
}

func WithFlusherLockFilename(name string) FileFlusherOption {
	return func(f *FileFlusher) { f.lockFilename = name }
}

func NewFileFlusher(dir string, target Emitter, opts ...FileFlusherOption) *FileFlusher {
	f := &FileFlusher{
		dir:          dir,
		ext:          DefaultFileExt,
		lockFilename: DefaultLockFilename,
		target:       target,
	}
	for _, o := range opts {
		o(f)
	}
	return f
}

func (f *FileFlusher) Flush(ctx context.Context) error {
	if _, err := os.Stat(f.dir); errors.Is(err, os.ErrNotExist) {
		return nil
	} else if err != nil {
		return err
	}

	lock, err := fslock.New(filepath.Join(f.dir, f.lockFilename))
	if err != nil {
		return err
	}
	if err := lock.TryLock(); err != nil {
		if errors.Is(err, fslock.ErrLocked) {
			return nil
		}
		return err
	}
	defer lock.Unlock()

	entries, err := os.ReadDir(f.dir)
	if err != nil {
		return err
	}

	if drainable, ok := f.target.(Drainable); ok {
		return f.flushDrainable(ctx, entries, drainable)
	}
	return f.flushSync(ctx, entries)
}

func (f *FileFlusher) flushSync(ctx context.Context, entries []os.DirEntry) error {
	for _, entry := range entries {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if entry.IsDir() || filepath.Ext(entry.Name()) != f.ext {
			continue
		}
		path := filepath.Join(f.dir, entry.Name())
		req, ok, err := f.readBatch(path)
		if err != nil {
			return err
		}
		if !ok {
			continue
		}
		if err := f.target.Send(ctx, req); err != nil {
			return err
		}
		if err := os.Remove(path); err != nil {
			return err
		}
	}
	return nil
}

func (f *FileFlusher) flushDrainable(ctx context.Context, entries []os.DirEntry, drainable Drainable) error {
	pathByEventID := map[string]string{}
	var paths []string

	for _, entry := range entries {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if entry.IsDir() || filepath.Ext(entry.Name()) != f.ext {
			continue
		}
		path := filepath.Join(f.dir, entry.Name())
		req, ok, err := f.readBatch(path)
		if err != nil {
			return err
		}
		if !ok {
			continue
		}
		if err := f.target.Send(ctx, req); err != nil {
			return err
		}
		for _, evt := range req.Events {
			pathByEventID[evt.ID] = path
		}
		paths = append(paths, path)
	}

	failed, drainErr := drainable.Drain(ctx)
	if drainErr != nil {
		return drainErr
	}

	failedPaths := map[string]struct{}{}
	for eventID := range failed {
		if p, ok := pathByEventID[eventID]; ok {
			failedPaths[p] = struct{}{}
		}
	}

	for _, path := range paths {
		if _, bad := failedPaths[path]; bad {
			continue
		}
		if err := os.Remove(path); err != nil {
			return err
		}
	}
	return drainErr
}

func (f *FileFlusher) readBatch(path string) (*LogEventsRequest, bool, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, false, err
	}
	if !CheckFilenameMD5(data, path, f.ext) {
		return nil, false, nil
	}
	var req LogEventsRequest
	if err := json.Unmarshal(data, &req); err != nil {
		return nil, false, nil
	}
	return &req, true, nil
}
