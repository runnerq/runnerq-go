package conductor

import (
	"context"
	"sync"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"

	"github.com/runnerq/runnerq-go/conductor/internal/wire"
	"github.com/runnerq/runnerq-go/executor"
)

const (
	noticeEvery  = 250 * time.Millisecond
	noticeBatch  = 500
	noticeBuffer = 5000
)

var noticeTypes = map[executor.ChangeKind]wire.NoticeType{
	executor.Created:          wire.NoticeCreated,
	executor.Scheduled:        wire.NoticeScheduled,
	executor.AttemptStarted:   wire.NoticeAttemptStarted,
	executor.AttemptSucceeded: wire.NoticeAttemptSucceeded,
}

// notices sends a session's activity.notices: the engine announces its
// lifecycle changes here while the Cloud asks for them (config.notices), and
// run sends what has gathered every noticeEvery. Past noticeBuffer waiting,
// the oldest go, counted in the next batch's dropped.
type notices struct {
	queue, executor string

	mu      sync.Mutex
	items   []wire.Notice
	dropped int64
}

func (n *notices) Announce(c executor.Change) {
	notice := wire.Notice{
		ActivityID:   c.ActivityID.String(),
		Type:         noticeTypes[c.Kind],
		At:           c.At.UTC().Format(time.RFC3339Nano),
		Queue:        n.queue,
		ActivityType: c.ActivityType,
		RootID:       c.RootID.String(),
		ExecutorID:   n.executor,
	}
	if c.Kind == executor.AttemptStarted || c.Kind == executor.AttemptSucceeded {
		notice.Attempt = c.Attempt
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	if len(n.items) >= noticeBuffer {
		n.items = n.items[1:]
		n.dropped++
	}
	n.items = append(n.items, notice)
}

func (n *notices) take() ([]wire.Notice, int64) {
	n.mu.Lock()
	defer n.mu.Unlock()
	items, dropped := n.items, n.dropped
	n.items, n.dropped = nil, 0
	return items, dropped
}

// run sends batches until ctx ends, each within the peer's frame limit.
func (n *notices) run(ctx context.Context, conn *websocket.Conn, frameLimit func() int64) {
	t := time.NewTicker(noticeEvery)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		items, dropped := n.take()
		for len(items) > 0 {
			size := min(len(items), noticeBatch)
			var evt wire.Envelope
			for {
				var err error
				if evt, err = newEvent(wire.TypeActivityNotices, wire.ActivityNotices{Items: items[:size], Dropped: dropped}); err != nil {
					return
				}
				if size == 1 || int64(len(evt.Data)) <= frameLimit()-1024 {
					break
				}
				size /= 2
			}
			wctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), writeTimeout)
			err := wsjson.Write(wctx, conn, evt)
			cancel()
			if err != nil {
				return
			}
			items, dropped = items[size:], 0
		}
	}
}
