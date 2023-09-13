// Package tail implements file tailing with fsnotify.
package tail

import (
	"bufio"
	"context"
	"io"
	"os"
	"syscall"
	"time"

	"github.com/fsnotify/fsnotify"
	"github.com/go-faster/errors"
	"go.uber.org/atomic"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// errStop is returned when the tail of a file has been marked to be stopped.
var errStop = errors.New("tail should now stop")

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

func (l *Line) final() bool {
	if l.isBlank() {
		return false
	}
	return l.Data[len(l.Data)-1] == '\n'
}

// Location returns corresponding Location for Offset.
//
// Mostly convenience helper for using as Config.Location.
func (l *Line) Location() Location {
	if l == nil {
		return Location{}
	}
	return Location{
		Offset: l.Offset,
		Whence: io.SeekStart,
	}
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
	// Initial internal buffer size, optional.
	BufferSize int
	// Logger to use, optional.
	Logger *zap.Logger
	// Tracker is optional custom *Tracker.
	Tracker Tracker
}

// Handler is called on each log line.
//
// Implementation should not retain Line or Line.Data.
type Handler func(ctx context.Context, l *Line) error

// Tracker tracks file changes.
type Tracker interface {
	watchFile(name string) error
	watchCreate(name string) error
	removeWatchName(name string) error
	removeWatchCreate(name string) error
	listenEvents(name string) <-chan fsnotify.Event
}

// Tailer implements file tailing.
//
// Use Tail() to start.
type Tailer struct {
	cfg     Config
	name    string
	file    *os.File
	reader  *bufio.Reader
	proxy   *offsetProxy
	watcher *watcher
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
	if cfg.Tracker == nil {
		cfg.Tracker = defaultTracker
	}

	return &Tailer{
		cfg:     cfg,
		name:    filename,
		lg:      cfg.Logger,
		watcher: newWatcher(cfg.Logger.Named("watch"), cfg.Tracker, filename),
	}
}

type offsetProxy struct {
	Reader io.Reader
	Offset int64
}

func (o *offsetProxy) Read(p []byte) (n int, err error) {
	n, err = o.Reader.Read(p)
	o.Offset += int64(n)
	return n, err
}

// offset returns the file's current offset.
func (t *Tailer) offset() int64 {
	return t.proxy.Offset - int64(t.reader.Buffered())
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
					return errors.Wrap(err, "wait exists")
				}

				continue
			}
			return errors.Wrap(err, "open")
		}
		offset, err := t.file.Seek(0, io.SeekCurrent)
		if err != nil {
			return errors.Wrap(err, "seek")
		}
		t.proxy = &offsetProxy{
			Reader: t.file,
			Offset: offset,
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

// Tail opens file and starts tailing it, reporting observed lines to Handler.
//
// Tail is blocking while calling Handler to reuse internal buffer and
// reduce allocations.
// Tail will call Handler in same sequence as lines are observed.
// See Handler for more info.
//
// Can be called multiple times, but not concurrently.
func (t *Tailer) Tail(ctx context.Context, h Handler) error {
	if t == nil {
		return errors.New("incorrect Tailer call: Tailer is nil")
	}

	defer t.closeFile()
	if err := t.openFile(ctx); err != nil {
		return errors.Wrap(err, "openFile")
	}

	if loc := t.cfg.Location; loc != nil {
		// Seek requested.
		if _, err := t.file.Seek(loc.Offset, loc.Whence); err != nil {
			return errors.Wrap(err, "seek")
		}
	}

	t.resetReader()
	t.lg.Debug("Opened")
	defer t.lg.Debug("Done")

	// Reading line-by-line.
	line := &Line{
		// Pre-allocate some buffer.
		// TODO(ernado): Limit buffer growth to prevent OOM
		Data: make([]byte, 0, t.cfg.BufferSize),
	}

	// Reduce lock contention.
	var done atomic.Bool
	go func() {
		<-ctx.Done()
		done.Store(true)
	}()

	for {
		if done.Load() {
			return ctx.Err()
		}

		// Grab the offset in case we need to back up in the event of a half-line.
		offset := t.offset()
		line.Offset = offset
		if e := t.lg.Check(zapcore.DebugLevel, "Offset"); e != nil {
			e.Write(zap.Int64("offset", offset))
		}

		var readErr error
		t.lg.Debug("Reading line")
		line.Data, readErr = t.readLine(line.Data)

		switch readErr {
		case io.EOF:
			t.lg.Debug("Got EOF")
			if line.final() {
				// Reporting only final lines, i.e. those ending with newline.
				// Line can become final later.
				if err := h(ctx, line); err != nil {
					return errors.Wrap(err, "handle")
				}
				line.Data = line.Data[:0] // reset buffer
			}
			if !t.cfg.Follow {
				// End of file reached, but not following.
				// Stopping.
				if !line.isBlank() && !line.final() {
					// Reporting non-final line because we are not following
					// and there are no chances for it to become final.
					if err := h(ctx, line); err != nil {
						return errors.Wrap(err, "handle")
					}
				}
				return nil
			}
			t.lg.Debug("Waiting for changes")
			if err := t.waitForChanges(ctx, offset); err != nil {
				if errors.Is(err, errStop) {
					return nil
				}
				if errors.Is(err, context.DeadlineExceeded) {
					continue
				}
				return errors.Wrap(err, "wait")
			}
		default:
			return errors.Wrap(readErr, "read")
		case nil:
			if err := h(ctx, line); err != nil {
				return errors.Wrap(err, "handle")
			}
			line.Data = line.Data[:0] // reset buffer
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

	if err := t.watcher.WatchEvents(ctx, pos, func(ctx context.Context, e event) error {
		switch e {
		case evModified:
			t.lg.Debug("Modified")
			return nil
		case evDeleted:
			t.lg.Debug("Stopping: deleted")
			return errStop
		case evTruncated:
			t.lg.Info("Re-opening truncated file")
			if err := t.openFile(ctx); err != nil {
				return errors.Wrap(err, "open file")
			}
			t.resetReader()
			return nil
		default:
			return errors.Errorf("invalid event %v", e)
		}
	}); err != nil {
		if os.IsNotExist(err) || errors.Is(err, syscall.ENOENT) {
			return errStop
		}
		return errors.Wrap(err, "watch")
	}

	return nil
}

func (t *Tailer) resetReader() {
	t.reader = bufio.NewReaderSize(t.proxy, t.cfg.BufferSize)
}
