package tail

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"
	"golang.org/x/sync/errgroup"
	"golang.org/x/xerrors"
)

const (
	lines         = 1024
	notifyTimeout = time.Millisecond * 500
	timeout       = time.Second * 5
	line          = `[foo.go:1261] INFO: Some test log entry {"user_id": 410}`
)

func file(t *testing.T) *os.File {
	t.Helper()

	f, err := os.CreateTemp(t.TempDir(), "*.txt")
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = f.Close()
	})
	t.Logf("Created test file %s", f.Name())

	return f
}

func TestTail_Run(t *testing.T) {
	t.Run("Follow", func(t *testing.T) {
		f := file(t)
		g, ctx := errgroup.WithContext(context.Background())

		var gotLines int
		h := func(ctx context.Context, l *Line) error {
			assert.Equal(t, line, string(l.Data))
			gotLines++
			t.Log("Got line", gotLines)

			if gotLines == lines {
				return ErrStop
			}

			return nil
		}

		g.Go(func() error {
			if err := File(f.Name(), Config{
				Follow:        true,
				Logger:        zaptest.NewLogger(t),
				NotifyTimeout: notifyTimeout,
			}).Tail(ctx, h); !xerrors.Is(err, ErrStop) {
				return xerrors.Errorf("run: %w", err)
			}

			return nil
		})
		g.Go(func() error {
			t.Log("Writing")

			for i := 0; i < lines; i++ {
				if _, err := fmt.Fprintln(f, line); err != nil {
					return xerrors.Errorf("write: %w", err)
				}
				if i%(lines/5) == 0 {
					if err := f.Sync(); err != nil {
						return xerrors.Errorf("sync: %w", err)
					}
				}
			}
			if err := f.Sync(); err != nil {
				return xerrors.Errorf("sync: %w", err)
			}
			if err := f.Close(); err != nil {
				return xerrors.Errorf("close: %w", err)
			}
			t.Log("Wrote")
			return nil
		})
		require.NoError(t, g.Wait())
		require.Equal(t, lines, gotLines)
	})
	t.Run("NoFollow", func(t *testing.T) {
		f := file(t)
		for i := 0; i < lines; i++ {
			_, err := fmt.Fprintln(f, line)
			require.NoError(t, err)
		}
		require.NoError(t, f.Close())

		// Perform full file read.
		ctx := context.Background()
		var gotLines int
		h := func(ctx context.Context, l *Line) error {
			assert.Equal(t, line, string(l.Data))
			gotLines++
			return nil
		}

		// Verify result.
		require.NoError(t, File(f.Name(), Config{}).Tail(ctx, h))
		require.Equal(t, lines, gotLines)
	})
	t.Run("Position", func(t *testing.T) {
		f := file(t)
		g, ctx := errgroup.WithContext(context.Background())

		var (
			gotLines int
			offset   int64
		)
		h := func(ctx context.Context, l *Line) error {
			assert.Equal(t, line, string(l.Data))
			gotLines++
			offset = l.Offset
			if gotLines == lines {
				return ErrStop
			}
			return nil
		}

		g.Go(func() error {
			if err := File(f.Name(), Config{
				Follow:        true,
				Logger:        zaptest.NewLogger(t),
				NotifyTimeout: notifyTimeout,
			}).Tail(ctx, h); !xerrors.Is(err, ErrStop) {
				return xerrors.Errorf("run: %w", err)
			}
			return nil
		})
		writeLines := func() error {
			for i := 0; i < lines; i++ {
				if _, err := fmt.Fprintln(f, line); err != nil {
					return xerrors.Errorf("write: %w", err)
				}
			}
			if err := f.Sync(); err != nil {
				return xerrors.Errorf("sync: %w", err)
			}
			return nil
		}
		g.Go(writeLines)
		require.NoError(t, g.Wait())
		require.Equal(t, lines, gotLines)

		require.NoError(t, writeLines())

		gotLines = 0
		require.ErrorIs(t, File(f.Name(), Config{
			Logger:   zaptest.NewLogger(t),
			Location: &Location{Offset: offset},
		}).Tail(context.Background(), h), ErrStop)
	})
}

func TestMultipleTails(t *testing.T) {
	f := file(t)

	lg := zaptest.NewLogger(t)
	tracker := NewTracker(lg)
	g, ctx := errgroup.WithContext(context.Background())

	const (
		tailers = 3
		lines   = 10
	)

	// Prepare multiple tailers and start them.
	for i := 0; i < tailers; i++ {
		tailer := File(f.Name(), Config{
			NotifyTimeout: notifyTimeout,
			Follow:        true,
			Logger:        lg.Named(fmt.Sprintf("t%d", i)),
			Tracker:       tracker,
		})
		g.Go(func() error {
			var gotLines int
			// Ensure that each tailer got all lines.
			h := func(ctx context.Context, l *Line) error {
				assert.Equal(t, line, string(l.Data))
				gotLines++
				if gotLines == lines {
					return ErrStop
				}
				return nil
			}

			ctx, cancel := context.WithTimeout(ctx, timeout)
			defer cancel()

			if err := tailer.Tail(ctx, h); !errors.Is(err, ErrStop) {
				return err
			}

			return nil
		})
	}
	// Write lines.
	g.Go(func() error {
		for i := 0; i < lines; i++ {
			if _, err := fmt.Fprintln(f, line); err != nil {
				return err
			}
		}
		return f.Close()
	})

	require.NoError(t, g.Wait())
}
