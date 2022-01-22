package tail

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/fsnotify/fsnotify"
	"github.com/go-faster/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"
	"golang.org/x/sync/errgroup"
)

type wrapTracker struct {
	file   func(name string)
	create func(name string)
	t      Tracker
}

func (w wrapTracker) watchFile(name string) error {
	if w.file != nil {
		defer w.file(name)
	}
	return w.t.watchFile(name)
}

func (w wrapTracker) watchCreate(name string) error {
	if w.create != nil {
		defer w.create(name)
	}
	return w.t.watchCreate(name)
}

func (w wrapTracker) removeWatchName(name string) error {
	return w.t.removeWatchName(name)
}

func (w wrapTracker) removeWatchCreate(name string) error {
	return w.t.removeWatchCreate(name)
}

func (w wrapTracker) listenEvents(name string) <-chan fsnotify.Event {
	return w.t.listenEvents(name)
}

func TestCreateAfterWatch(t *testing.T) {
	lg := zaptest.NewLogger(t)
	g, ctx := errgroup.WithContext(context.Background())
	name := filepath.Join(t.TempDir(), "foo.txt")

	const lines = 10

	started := make(chan struct{})
	g.Go(func() error {
		select {
		case <-started:
		case <-ctx.Done():
			return ctx.Err()
		}

		f, err := os.Create(name)
		if err != nil {
			return err
		}
		for i := 0; i < lines; i++ {
			if _, err := fmt.Fprintln(f, line); err != nil {
				return err
			}
		}
		return f.Close()
	})

	tailer := File(name, Config{
		NotifyTimeout: notifyTimeout,
		Follow:        true,
		Logger:        lg,
		Tracker: wrapTracker{
			t: NewTracker(lg),
			create: func(name string) {
				close(started)
			},
		},
	})

	read := make(chan struct{})
	g.Go(func() error {
		var gotLines int
		// Ensure that each tailer got all lines.
		h := func(ctx context.Context, l *Line) error {
			assert.Equal(t, line, string(l.Data))
			gotLines++
			if gotLines == lines {
				close(read)
			}
			return nil
		}

		ctx, cancel := context.WithTimeout(ctx, timeout)
		defer cancel()

		if err := tailer.Tail(ctx, h); !errors.Is(err, errStop) {
			return err
		}

		return nil
	})

	// Read lines.
	g.Go(func() error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-read: // ok
		}
		return os.Remove(name)
	})

	require.NoError(t, g.Wait())
}
