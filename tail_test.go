package tail

import (
	"context"
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

func TestTail_Run(t *testing.T) {
	t.Run("Follow", func(t *testing.T) {
		// Create test file.
		f, err := os.CreateTemp(t.TempDir(), "*.txt")
		require.NoError(t, err)
		name := f.Name()
		t.Log(name)
		const lines int = 512

		g, ctx := errgroup.WithContext(context.Background())

		var gotLines int
		h := func(ctx context.Context, l *Line) error {
			assert.Equal(t, "Some line", string(l.Data))
			gotLines++
			t.Log("Got line", gotLines)

			if gotLines == lines {
				return ErrStop
			}

			return nil
		}

		g.Go(func() error {
			if err := File(name, Config{
				Follow:        true,
				Logger:        zaptest.NewLogger(t),
				NotifyTimeout: time.Millisecond * 500,
			}).Tail(ctx, h); !xerrors.Is(err, ErrStop) {
				return xerrors.Errorf("run: %w", err)
			}

			return nil
		})
		g.Go(func() error {
			t.Log("Writing")

			for i := 0; i < lines; i++ {
				if _, err := fmt.Fprintln(f, "Some line"); err != nil {
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
		// Prepare test file.
		f, err := os.CreateTemp(t.TempDir(), "*.txt")
		require.NoError(t, err)
		name := f.Name()
		t.Log(name)
		const lines int = 1024
		for i := 0; i < lines; i++ {
			_, err := fmt.Fprintln(f, "Some line")
			require.NoError(t, err)
		}
		require.NoError(t, f.Close())

		// Perform full file read.
		ctx := context.Background()
		var gotLines int
		h := func(ctx context.Context, l *Line) error {
			assert.Equal(t, "Some line", string(l.Data))
			gotLines++
			return nil
		}

		// Verify result.
		require.NoError(t, File(name, Config{}).Tail(ctx, h))
		require.Equal(t, lines, gotLines)
	})
	t.Run("Position", func(t *testing.T) {
		f, err := os.CreateTemp(t.TempDir(), "*.txt")
		require.NoError(t, err)
		defer func() { _ = f.Close() }()

		name := f.Name()
		t.Log(name)
		const lines int = 512

		g, ctx := errgroup.WithContext(context.Background())

		var (
			gotLines int
			offset   int64
		)
		h := func(ctx context.Context, l *Line) error {
			assert.Equal(t, "Some line", string(l.Data))
			gotLines++
			offset = l.Offset
			if gotLines == lines {
				return ErrStop
			}
			return nil
		}

		g.Go(func() error {
			if err := File(name, Config{
				Follow:        true,
				Logger:        zaptest.NewLogger(t),
				NotifyTimeout: time.Millisecond * 500,
			}).Tail(ctx, h); !xerrors.Is(err, ErrStop) {
				return xerrors.Errorf("run: %w", err)
			}
			return nil
		})
		writeLines := func() error {
			for i := 0; i < lines; i++ {
				if _, err := fmt.Fprintln(f, "Some line"); err != nil {
					return xerrors.Errorf("write: %w", err)
				}
			}
			if err := f.Sync(); err != nil {
				return xerrors.Errorf("clise: %w", err)
			}
			return nil
		}
		g.Go(writeLines)
		require.NoError(t, g.Wait())
		require.Equal(t, lines, gotLines)

		require.NoError(t, writeLines())

		gotLines = 0
		require.ErrorIs(t, File(name, Config{
			Logger:   zaptest.NewLogger(t),
			Location: &Location{Offset: offset},
		}).Tail(context.Background(), h), ErrStop)
	})
}
