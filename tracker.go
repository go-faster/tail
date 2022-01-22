package tail

import (
	"os"
	"path/filepath"
	"sync"
	"syscall"

	"github.com/fsnotify/fsnotify"
	"github.com/go-faster/errors"
	"go.uber.org/zap"
)

// tracker multiplexes fsnotify events.
type tracker struct {
	init      sync.Once
	mux       sync.Mutex
	watcher   *fsnotify.Watcher
	chans     map[string]chan fsnotify.Event
	done      map[string]chan bool
	watchNums map[string]int
	watch     chan *watchInfo
	remove    chan *watchInfo
	error     chan error
	log       *zap.Logger
}

type watchInfo struct {
	op   fsnotify.Op
	name string
}

func (i *watchInfo) isCreate() bool {
	return i.op == fsnotify.Create
}

// NewTracker creates new custom Tracker with provided logger.
//
// It is recommended to use it as singleton and create only once.
func NewTracker(log *zap.Logger) Tracker {
	return &tracker{
		chans:     make(map[string]chan fsnotify.Event),
		done:      make(map[string]chan bool),
		watchNums: make(map[string]int),
		watch:     make(chan *watchInfo),
		remove:    make(chan *watchInfo),
		error:     make(chan error),
		log:       log,
	}
}

var defaultTracker = NewTracker(zap.NewNop())

// watchFile signals the run goroutine to begin watching the input filename
func (t *tracker) watchFile(name string) error {
	return t.watchInfo(&watchInfo{
		name: name,
	})
}

// watchCreate watches create signals the run goroutine to begin watching the input filename
// if call the watchCreate function, don't call the Cleanup, call the removeWatchCreate
func (t *tracker) watchCreate(name string) error {
	return t.watchInfo(&watchInfo{
		op:   fsnotify.Create,
		name: name,
	})
}

func (t *tracker) watchInfo(winfo *watchInfo) error {
	if err := t.ensure(); err != nil {
		return err
	}

	winfo.name = filepath.Clean(winfo.name)
	t.watch <- winfo
	return <-t.error
}

// removeWatchInfo signals the run goroutine to remove the watch for the input filename
func (t *tracker) removeWatchName(name string) error {
	return t.removeInfo(&watchInfo{
		name: name,
	})
}

// removeWatchCreate signals the run goroutine to remove the
// watch for the input filename.
func (t *tracker) removeWatchCreate(name string) error {
	return t.removeInfo(&watchInfo{
		op:   fsnotify.Create,
		name: name,
	})
}

func (t *tracker) ensure() (err error) {
	if t == nil {
		return errors.New("tracker: invalid call (nil)")
	}

	t.init.Do(func() {
		w, wErr := fsnotify.NewWatcher()
		if wErr != nil {
			err = wErr
			return
		}

		t.watcher = w
		go t.run()
	})
	return err
}

func (t *tracker) removeInfo(winfo *watchInfo) error {
	if err := t.ensure(); err != nil {
		return err
	}

	winfo.name = filepath.Clean(winfo.name)
	t.mux.Lock()
	done := t.done[winfo.name]
	if done != nil {
		delete(t.done, winfo.name)
		close(done)
	}
	t.mux.Unlock()

	t.remove <- winfo
	return <-t.error
}

// listenEvents returns a channel to which FileEvents corresponding to the input filename
// will be sent. This channel will be closed when removeWatchInfo is called on this
// filename.
func (t *tracker) listenEvents(name string) <-chan fsnotify.Event {
	t.mux.Lock()
	defer t.mux.Unlock()

	return t.chans[name]
}

// watchFlags calls fsnotify.WatchFlags for the input filename and flags, creating
// a new watcher if the previous watcher was closed.
func (t *tracker) addWatchInfo(winfo *watchInfo) error {
	t.mux.Lock()
	defer t.mux.Unlock()

	if t.chans[winfo.name] == nil {
		t.chans[winfo.name] = make(chan fsnotify.Event)
	}
	if t.done[winfo.name] == nil {
		t.done[winfo.name] = make(chan bool)
	}

	name := winfo.name
	if winfo.isCreate() {
		// watchFile for new files to be created in the parent directory.
		name = filepath.Dir(name)
	}

	if t.watchNums[name] > 0 {
		// Already watching.
		return nil
	}
	if err := t.watcher.Add(name); err != nil {
		return errors.Wrap(err, "add")
	}

	t.watchNums[name]++
	return nil
}

// removeWatchInfo calls fsnotify.Remove for the input filename and closes the
// corresponding events channel.
func (t *tracker) removeWatchInfo(winfo *watchInfo) error {
	t.mux.Lock()

	ch := t.chans[winfo.name]
	if ch != nil {
		delete(t.chans, winfo.name)
		close(ch)
	}

	name := winfo.name
	if winfo.isCreate() {
		// watchFile for new files to be created in the parent directory.
		name = filepath.Dir(name)
	}
	t.watchNums[name]--
	watchNum := t.watchNums[name]
	if watchNum == 0 {
		delete(t.watchNums, name)
	}
	t.mux.Unlock()

	var err error
	// If we were the last ones to watch this file, unsubscribe from newWatcher.
	// This needs to happen after releasing the lock because fsnotify waits
	// synchronously for the kernel to acknowledge the removal of the watch
	// for this file, which causes us to deadlock if we still held the lock.
	if watchNum == 0 {
		err = t.watcher.Remove(name)
	}

	return err
}

// sendEvent sends the input event to the appropriate Tail.
func (t *tracker) sendEvent(event fsnotify.Event) {
	name := filepath.Clean(event.Name)

	t.mux.Lock()
	ch := t.chans[name]
	done := t.done[name]
	t.mux.Unlock()

	if ch != nil && done != nil {
		select {
		case ch <- event:
		case <-done:
		}
	}
}

// run starts reading from inotify events.
func (t *tracker) run() {
	for {
		select {
		case winfo := <-t.watch:
			t.error <- t.addWatchInfo(winfo)

		case winfo := <-t.remove:
			t.error <- t.removeWatchInfo(winfo)

		case event, ok := <-t.watcher.Events:
			if !ok {
				return
			}
			t.sendEvent(event)

		case err, ok := <-t.watcher.Errors:
			if !ok {
				return
			}
			if err != nil {
				sysErr, ok := err.(*os.SyscallError)
				if !ok || sysErr.Err != syscall.EINTR {
					t.log.Error("Watcher error", zap.Error(err))
				}
			}
		}
	}
}
