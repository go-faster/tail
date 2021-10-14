// Package tail implements file tailing with fsnotify.
package tail

import (
	"bufio"
	"context"
	"errors"
	"io"
	"os"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"golang.org/x/xerrors"
)

// ErrStop is returned when the tail of a file has been marked to be stopped.
var ErrStop = errors.New("tail should now stop")

// Line of file.
type Line struct {
	Data   []byte // do not retain, reused while reading file
	Offset int64  // is always the offset from start
}

// isBlank reports whether line is blank.
func (l *Line) isBlank() bool {
	if l == nil {
		return true
	}
	return len(l.Data) == 0
}

// Location represents arguments to io.Seek.
//
// See https://golang.org/pkg/io/#SectionReader.Seek
type Location struct {
	Offset int64
	Whence int
}

// Config is used to specify how a file must be tailed.
type Config struct {
	// Location sets starting file location.
	Location *Location
	// NotifyTimeout enables additional timeout for file changes waiting.
	// Can be used to ensure that we never miss event even if newWatcher fails to
	// deliver event.
	// Optional.
	NotifyTimeout time.Duration
	// Follow file after reaching io.EOF, waiting for new lines.
	Follow bool
	// BufferSize for internal reader, optional.
	BufferSize int
	// Logger to use, optional.
	Logger *zap.Logger
}

// Handler is called on each log line.
//
// Implementation should not retain Line or Line.Data.
type Handler func(ctx context.Context, l *Line) error

// fileWatcher monitors file events.
type fileWatcher interface {
	// WaitExists blocks until the file comes into existence.
	WaitExists(ctx context.Context) error

	// WatchEvents calls h when file is truncated, deleted or modified.
	WatchEvents(ctx context.Context, offset int64, h watchHandler) error
}

// Tailer implements file tailing.
//
// Use Tail() to start.
type Tailer struct {
	cfg     Config
	name    string
	file    *os.File
	reader  *bufio.Reader
	watcher fileWatcher
	lg      *zap.Logger
}

const (
	minBufSize     = 128       // 128 bytes
	defaultBufSize = 1024 * 50 // 50kb
)

// File configures and creates new unstarted *Tailer.
//
// Use Tailer.Tail() to start tailing file.
func File(filename string, cfg Config) *Tailer {
	if cfg.Logger == nil {
		cfg.Logger = zap.NewNop()
	}
	if cfg.BufferSize <= minBufSize {
		cfg.BufferSize = defaultBufSize
	}
	return &Tailer{
		cfg:  cfg,
		name: filename,
		lg:   cfg.Logger,
	}
}

// offset returns the file's current offset and error if any.
//
// NB: can be not accurate
// TODO(ernado): what does it mean exactly?
func (t *Tailer) offset() (offset int64, err error) {
	offset, err = t.file.Seek(0, io.SeekCurrent)
	if err != nil {
		return offset, xerrors.Errorf("seek: %w", err)
	}
	offset -= int64(t.reader.Buffered())
	return offset, nil
}

func (t *Tailer) closeFile() {
	if t.file == nil {
		return
	}

	_ = t.file.Close()
	t.file = nil
}

func (t *Tailer) openFile(ctx context.Context) error {
	t.closeFile()
	for {
		var err error
		if t.file, err = os.Open(t.name); err != nil {
			if os.IsNotExist(err) {
				if e := t.lg.Check(zapcore.DebugLevel, "File does not exists"); e != nil {
					e.Write(
						zap.Error(err),
						zap.String("tail.file", t.name),
					)
				}
				if err := t.watcher.WaitExists(ctx); err != nil {
					return xerrors.Errorf("wait exists: %w", err)
				}

				continue
			}
			return xerrors.Errorf("open: %w", err)
		}
		return nil
	}
}

func (t *Tailer) readLine(buf []byte) ([]byte, error) {
	for {
		line, isPrefix, err := t.reader.ReadLine()
		buf = append(buf, line...)
		if isPrefix {
			continue
		}
		if err != nil {
			return nil, err
		}
		return buf, nil
	}
}

// Tail opens file and starts tailing it, reporting observed lines to watchHandler.
//
// Tail is blocking while calling watchHandler to reuse internal buffer and
// reduce allocations.
// Tail will call watchHandler in same sequence as lines are observed.
// See watchHandler for more info.
//
// Can be called multiple times, but not concurrently.
func (t *Tailer) Tail(ctx context.Context, h Handler) error {
	if t == nil {
		return xerrors.New("incorrect Tailer call: Tailer is nil")
	}

	t.lg.Debug("Using newWatcher")
	t.watcher = newWatcher(t.lg.Named("watch"), t.name)

	defer t.closeFile()
	if err := t.openFile(ctx); err != nil {
		return xerrors.Errorf("openFile: %w", err)
	}

	if loc := t.cfg.Location; loc != nil {
		// Seek requested.
		if _, err := t.file.Seek(loc.Offset, loc.Whence); err != nil {
			return xerrors.Errorf("seek: %w", err)
		}
	}

	t.resetReader()
	t.lg.Debug("Opened")

	// Reading line-by-line.
	line := &Line{
		// Pre-allocate some buffer.
		Data: make([]byte, 0, t.cfg.BufferSize),
	}

	for {
		if err := ctx.Err(); err != nil {
			return ctx.Err()
		}

		// Grab the offset in case we need to back up in the event of a half-line.
		t.lg.Debug("Offset")
		offset, err := t.offset()
		if err != nil {
			return xerrors.Errorf("offset: %w", err)
		}

		line.Offset = offset

		var readErr error
		t.lg.Debug("Reading line")
		line.Data, readErr = t.readLine(line.Data[:0])

		// Remove newline at the end of lineData.
		if len(line.Data) > 0 && line.Data[len(line.Data)-1] == '\n' {
			line.Data = line.Data[:len(line.Data)-1]
		}

		switch readErr {
		case io.EOF:
			t.lg.Debug("Got EOF")
			if !line.isBlank() {
				// Reporting new non-blank line.
				if err := h(ctx, line); err != nil {
					return xerrors.Errorf("handle: %w", err)
				}
			}
			if !t.cfg.Follow {
				// End of file reached, but not following.
				// Stopping.
				return nil
			}
			t.lg.Debug("Waiting for changes")
			if err := t.waitForChanges(ctx, offset); err != nil {
				if xerrors.Is(err, ErrStop) || xerrors.Is(err, context.DeadlineExceeded) {
					continue
				}
				return xerrors.Errorf("wait: %w", err)
			}
		default:
			return xerrors.Errorf("read: %w", readErr)
		case nil:
			if err := h(ctx, line); err != nil {
				return xerrors.Errorf("handle: %w", err)
			}
		}
	}
}

// waitForChanges waits until the file has been appended, deleted,
// moved or truncated.
//
// evTruncated files are always reopened.
func (t *Tailer) waitForChanges(ctx context.Context, pos int64) error {
	if t.cfg.NotifyTimeout != 0 {
		// Additional safeguard to ensure that we don't hang forever.
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, t.cfg.NotifyTimeout)
		defer cancel()
	}

	return t.watcher.WatchEvents(ctx, pos, func(ctx context.Context, e event) error {
		switch e {
		case evModified:
			t.lg.Debug("Modified")
			return nil
		case evDeleted:
			t.lg.Debug("Stopping: deleted")
			return ErrStop
		case evTruncated:
			t.lg.Info("Re-opening truncated file")
			if err := t.openFile(ctx); err != nil {
				return xerrors.Errorf("open file: %w", err)
			}
			t.resetReader()
			return nil
		default:
			return xerrors.Errorf("invalid event %v", e)
		}
	})
}

func (t *Tailer) resetReader() { t.reader = bufio.NewReaderSize(t.file, t.cfg.BufferSize) }
