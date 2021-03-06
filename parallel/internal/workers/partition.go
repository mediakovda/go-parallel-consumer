package workers

import (
	"context"
	"fmt"

	"github.com/mediakovda/go-parallel-consumer/parallel/internal/events"
	"github.com/mediakovda/go-parallel-consumer/parallel/internal/hashmap"
)

// partitionWorker handles processing of messages from one partition.
type partitionWorker struct {
	context context.Context
	cancel  context.CancelFunc

	Messages  chan *events.Message
	processor Processor
	done      chan<- *events.Message

	offsets       chan<- events.OffsetUpdate
	offsetTracker *offsetTracker

	jobs    *hashmap.HashMap // map[*jobKey]*job
	running int
	jobDone chan *job
}

type partitionParams struct {
	Messages  chan *events.Message
	Processor Processor

	Offsets chan<- events.OffsetUpdate
	Done    chan<- *events.Message
}

func newPartition(ctx context.Context, p *partitionParams) *partitionWorker {
	w := &partitionWorker{
		Messages:  p.Messages,
		processor: p.Processor,
		done:      p.Done,

		offsets: p.Offsets,

		jobs:    hashmap.New(),
		jobDone: make(chan *job),
	}
	w.context, w.cancel = context.WithCancel(ctx)

	return w
}

func (w *partitionWorker) Run() {
loop:
	for {
		select {
		case m, ok := <-w.Messages:
			if !ok {
				break loop
			}
			w.handleMessage(m)

		case j := <-w.jobDone:
			w.handleJobDone(j)
		}
	}

	// wait for jobs to finish
	// we don't want them to stuck on sending to w.jobDone
	for w.running > 0 {
		j := <-w.jobDone
		w.handleJobDone(j)
	}
	close(w.jobDone)
}

func (w *partitionWorker) Close() {
	w.cancel()
	close(w.Messages)
}

func (w *partitionWorker) handleMessage(m *events.Message) {
	// we can only find out initial offset in first message
	if w.offsetTracker == nil {
		w.offsetTracker = newOffsetTracker(int(m.Offset))
	}

	k := newJobKey(m.Key)

	if !k.IsEmpty() {
		v := w.jobs.Get(k)

		if v != nil {
			j := v.(*job)
			j.attach(m)
			return
		}
	}

	j := newJob(k, m)
	w.startJob(j)
}

func (w *partitionWorker) handleJobDone(j *job) {
	if !w.jobSucceeded(j) {
		return
	}

	w.updateOffset(j.current)

	nextJob := j.next()

	if nextJob == nil {
		w.jobs.Pop(j.key)
		return
	}

	w.startJob(nextJob)
}

func (w *partitionWorker) updateOffset(m *events.Message) {
	move, _ := w.offsetTracker.Mark(int(m.Offset))
	if move != -1 {
		o := events.OffsetUpdate{
			Topic:     m.Topic,
			Partition: m.Partition,
			Offset:    int64(move),
		}
		w.offsets <- o
	}
}

func (w *partitionWorker) startJob(j *job) {
	w.running += 1

	if !j.key.IsEmpty() {
		w.jobs.Put(j.key, j)
	}

	go func() {
		j.run(w.context, w.processor)
		w.jobDone <- j
	}()
}

func (w *partitionWorker) jobSucceeded(j *job) bool {
	w.running -= 1
	w.done <- j.current

	if j.err != nil {
		for _, rest := range j.attached {
			w.done <- rest
		}

		if j.err == context.Canceled || j.err == context.DeadlineExceeded {
			return false
		}

		// job failed because of an error in w.processor function
		// and we can't do anything at this point
		panic(fmt.Errorf("processor function failed: %w", j.err))
	}

	return true
}
