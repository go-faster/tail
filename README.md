# tail [![Go Reference](https://img.shields.io/badge/go-pkg-00ADD8)](https://pkg.go.dev/github.com/go-faster/tail#section-documentation) [![codecov](https://img.shields.io/codecov/c/github/go-faster/tail?label=cover)](https://codecov.io/gh/go-faster/tail) [![experimental](https://img.shields.io/badge/-experimental-blueviolet)](https://go-faster.org/docs/projects/status#experimental)

Package tail implements file tailing with [fsnotify](https://github.com/fsnotify/fsnotify).

Fork of [nxadm/tail](https://github.com/nxadm/tail), simplified, reworked and optimized.
Currently, supports only Linux and Darwin.

```console
go get github.com/go-faster/tail
```

```go
package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/go-faster/tail"
)

func main() {
	t := tail.File("/var/log/application.txt", tail.Config{
		Follow:        true,        // tail -f
		BufferSize:    1024 * 128,  // 128 kb for internal reader buffer

		// Force polling if zero events are observed for longer than a minute.
		// Optional, just a safeguard to be sure that we are not stuck forever
		// if we miss inotify event.
		NotifyTimeout: time.Minute,

		// You can specify position to start tailing, same as Seek arguments.
		// For example, you can use the latest processed Line.Location() value.
		Location: &tail.Location{Whence: io.SeekStart, Offset: 0},
	})
	ctx := context.Background()
	// Enjoy zero allocation fast tailing with context support.
	if err := t.Tail(ctx, func(ctx context.Context, l *tail.Line) error {
		_, _ = fmt.Fprintln(os.Stdout, string(l.Data))
		return nil
	}); err != nil {
		panic(err)
	}
}
```

## TODO
- [ ] Decide on ergonomics of ErrStop
- [ ] Tests for removing, tailing and creating events
- [x] Benchmarks
- [ ] Decide how to deal with partial lines (i.e. EOF, but without `\n`)
- [ ] Decide on Windows support
