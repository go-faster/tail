package tail

import (
	"os"
	"path/filepath"
	"sync"
	"syscall"

	"github.com/fsnotify/fsnotify"
	"go.uber.org/zap"
)

type inotifyTracker struct {
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

var (
	// globally shared inotifyTracker; ensures only one fsnotify.watcher is used
	shared *inotifyTracker

	// these are used to ensure the shared inotifyTracker is run exactly once
	once  = sync.Once{}
	goRun = func() {
		shared = &inotifyTracker{
			mux:       sync.Mutex{},
			chans:     make(map[string]chan fsnotify.Event),
			done:      make(map[string]chan bool),
			watchNums: make(map[string]int),
			watch:     make(chan *watchInfo),
			remove:    make(chan *watchInfo),
			error:     make(chan error),

			// TODO: Allow changing logger
			log: zap.NewNop(),
		}
		go shared.run()
	}
)

// watchFile signals the run goroutine to begin watching the input filename
func watchFile(name string) error {
	return watch(&watchInfo{
		name: name,
	})
}

// watchCreate watches create signals the run goroutine to begin watching the input filename
// if call the watchCreate function, don't call the Cleanup, call the removeWatchCreate
func watchCreate(name string) error {
	return watch(&watchInfo{
		op:   fsnotify.Create,
		name: name,
	})
}

func watch(winfo *watchInfo) error {
	// start running the shared inotifyTracker if not already running
	once.Do(goRun)

	winfo.name = filepath.Clean(winfo.name)
	shared.watch <- winfo
	return <-shared.error
}

// removeWatch signals the run goroutine to remove the watch for the input filename
func removeWatch(name string) error {
	return remove(&watchInfo{
		name: name,
	})
}

// removeWatchCreate signals the run goroutine to remove the
// watch for the input filename.
func removeWatchCreate(name string) error {
	return remove(&watchInfo{
		op:   fsnotify.Create,
		name: name,
	})
}

func remove(winfo *watchInfo) error {
	// start running the shared inotifyTracker if not already running
	once.Do(goRun)

	winfo.name = filepath.Clean(winfo.name)
	shared.mux.Lock()
	done := shared.done[winfo.name]
	if done != nil {
		delete(shared.done, winfo.name)
		close(done)
	}
	shared.mux.Unlock()

	shared.remove <- winfo
	return <-shared.error
}

// listenEvents returns a channel to which FileEvents corresponding to the input filename
// will be sent. This channel will be closed when removeWatch is called on this
// filename.
func listenEvents(name string) <-chan fsnotify.Event {
	shared.mux.Lock()
	defer shared.mux.Unlock()

	return shared.chans[name]
}

// watchFlags calls fsnotify.WatchFlags for the input filename and flags, creating
// a new watcher if the previous watcher was closed.
func (t *inotifyTracker) addWatch(winfo *watchInfo) error {
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

	var err error
	// already in newWatcher watch
	if t.watchNums[name] == 0 {
		err = t.watcher.Add(name)
	}
	if err == nil {
		t.watchNums[name]++
	}
	return err
}

// removeWatch calls fsnotify.removeWatch for the input filename and closes the
// corresponding events channel.
func (t *inotifyTracker) removeWatch(winfo *watchInfo) error {
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
func (t *inotifyTracker) sendEvent(event fsnotify.Event) {
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

// run starts the goroutine in which the shared struct reads events from its
// watcher's Event channel and sends the events to the appropriate Tail.
func (t *inotifyTracker) run() {
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		panic(err)
	}
	t.watcher = watcher

	for {
		select {
		case winfo := <-t.watch:
			t.error <- t.addWatch(winfo)

		case winfo := <-t.remove:
			t.error <- t.removeWatch(winfo)

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
