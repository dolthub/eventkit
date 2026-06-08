package eventkit

import (
	"context"
	"errors"
	"os"
	"path/filepath"
)

const DefaultLockFilename = "eventkit.lock"

type FileEmitter struct {
	dir   string
	ext   string
	codec Codec
}

type FileEmitterOption func(*FileEmitter)

func WithFileExtension(ext string) FileEmitterOption {
	return func(f *FileEmitter) { f.ext = ext }
}

func WithFileCodec(c Codec) FileEmitterOption {
	return func(f *FileEmitter) { f.codec = c }
}

func NewFileEmitter(dir string, opts ...FileEmitterOption) (*FileEmitter, error) {
	if dir == "" {
		return nil, errors.New("eventkit: FileEmitter requires a non-empty dir")
	}
	fe := &FileEmitter{
		dir:   dir,
		ext:   DefaultFileExt,
		codec: DefaultCodec,
	}
	for _, o := range opts {
		o(fe)
	}
	if fe.codec == nil {
		fe.codec = DefaultCodec
	}
	if err := os.MkdirAll(fe.dir, 0o755); err != nil {
		return nil, err
	}
	return fe, nil
}

func (f *FileEmitter) Send(_ context.Context, req *LogEventsRequest) error {
	if req == nil || len(req.Events) == 0 {
		return nil
	}
	data, err := f.codec.Marshal(req)
	if err != nil {
		return err
	}

	tmp, err := os.CreateTemp(f.dir, ".write-*")
	if err != nil {
		return err
	}
	tmpPath := tmp.Name()
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		os.Remove(tmpPath)
		return err
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		os.Remove(tmpPath)
		return err
	}
	if err := tmp.Close(); err != nil {
		os.Remove(tmpPath)
		return err
	}

	finalPath := filepath.Join(f.dir, Filename(data, f.ext))
	if err := os.Rename(tmpPath, finalPath); err != nil {
		os.Remove(tmpPath)
		return err
	}
	return nil
}
