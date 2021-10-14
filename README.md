# Tail

Package tail implements file tailing with [fsnotify](github.com/fsnotify/fsnotify).

Fork of [nxadm/tail](https://github.com/nxadm/tail), simplified, reworked and optimized.
Fully supports only posix-compatible OS-es.

```console
go get github.com/ernado/tail
```

```go
package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/ernado/tail"
)

func main() {
	t := tail.File("/var/log/application.txt", tail.Config{
		Follow:        true,        // tail -f
		NotifyTimeout: time.Minute, // force pooling at least one minute
		BufferSize:    1024 * 128,  // 128 kb for internal reader buffer

		// You can specify position to start tailing, same as Seek arguments.
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
