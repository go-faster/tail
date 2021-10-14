package tail

import (
	"context"
	"os"
	"path/filepath"

	"github.com/fsnotify/fsnotify"
	"go.uber.org/zap"
	"golang.org/x/xerrors"
)

// event that happened with file.
type event int

// Possible file events.
const (
	evModified event = iota
	evTruncated
	evDeleted
)

func (c event) String() string {
	switch c {
	case evDeleted:
		return "deleted"
	case evTruncated:
		return "truncated"
	default:
		return "modified"
	}
}

// watchHandler is called on event to file.
type watchHandler func(ctx context.Context, e event) error

// watcher uses newWatcher to monitor file changes.
type watcher struct {
	lg   *zap.Logger
	name string
	size int64
}

func newWatcher(lg *zap.Logger, filename string) *watcher {
	return &watcher{
		name: filepath.Clean(filename),
		size: 0,
		lg:   lg,
	}
}

func (w *watcher) WaitExists(ctx context.Context) error {
	if err := watchCreate(w.name); err != nil {
		return xerrors.Errorf("create: %w", err)
	}
	defer func() {
		if err := removeWatchCreate(w.name); err != nil {
			w.lg.Debug("Failed to remove create event handler", zap.Error(err))
		}
	}()

	// Check that file is already exists.
	if _, err := os.Stat(w.name); !os.IsNotExist(err) {
		// File exists, or stat returned an error.
		return err
	}

	events := listenEvents(w.name)

	for {
		select {
		case evt, ok := <-events:
			if !ok {
				return xerrors.New("newWatcher watcher has been closed")
			}
			evtName, err := filepath.Abs(evt.Name)
			if err != nil {
				return xerrors.Errorf("abs: %w", err)
			}
			fwFilename, err := filepath.Abs(w.name)
			if err != nil {
				return xerrors.Errorf("abs: %w", err)
			}
			if evtName == fwFilename {
				return nil
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (w *watcher) WatchEvents(ctx context.Context, offset int64, fn watchHandler) error {
	if err := watchFile(w.name); err != nil {
		return xerrors.Errorf("watch: %w", err)
	}

	w.size = offset
	events := listenEvents(w.name)
	defer func() {
		if err := removeWatch(w.name); err != nil {
			w.lg.Debug("Failed to remove event handler", zap.Error(err))
		}
	}()

	for {
		prevSize := w.size

		var (
			evt fsnotify.Event
			ok  bool
		)
		select {
		case evt, ok = <-events:
			if !ok {
				return nil
			}
		case <-ctx.Done():
			return ctx.Err()
		}

		switch {
		case evt.Op&fsnotify.Remove == fsnotify.Remove:
			fallthrough

		case evt.Op&fsnotify.Rename == fsnotify.Rename:
			return fn(ctx, evDeleted)

		// With an open fd, unlink(fd) - newWatcher returns IN_ATTRIB (==fsnotify.Chmod)
		case evt.Op&fsnotify.Chmod == fsnotify.Chmod:
			fallthrough

		case evt.Op&fsnotify.Write == fsnotify.Write:
			fi, err := os.Stat(w.name)
			if err != nil {
				if os.IsNotExist(err) {
					return fn(ctx, evDeleted)
				}
				return xerrors.Errorf("stat: %w", err)
			}
			w.size = fi.Size()
			if prevSize > 0 && prevSize > w.size {
				return fn(ctx, evTruncated)
			}
			return fn(ctx, evModified)
		}
	}
}
