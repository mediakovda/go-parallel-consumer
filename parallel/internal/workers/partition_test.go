package workers

import (
	"context"
	"math/rand"
	"testing"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

// TestPartitionWorkerDone tests all received messages returned through .done channel
func TestPartitionWorkerDone(t *testing.T) {
	n := 999
	w := newTestPartitionWorker(n)

	done := make(chan *kafka.Message, n)
	w.done = done
	w.processor = func(ctx context.Context, m *kafka.Message) {
		if rand.Float64() < 0.33 {
			// for this test we want some of them to stuck in unprocessed state
			<-ctx.Done()
		}
	}

	go w.Run()
	ensureAllMessagesConsumed(w)

	w.Close()

	messages := readMessages(done, n, time.Second)

	if len(messages) != n {
		t.Errorf("%d messages got lost, PartitionWorker should return all message to .done", n-len(messages))
	}
}

func TestOffsets(t *testing.T) {
	n := 999
	w := newTestPartitionWorker(n)

	done := make(chan *kafka.Message, n)
	offsets := make(chan kafka.TopicPartition, n)
	w.done = done
	w.offsets = offsets

	go w.Run()
	readMessages(done, n, time.Second)

	close(w.offsets)
	var lastOffset kafka.TopicPartition
	for lastOffset = range offsets {
	}

	if int(lastOffset.Offset) != n {
		// offset of the last message is n-1
		// so broker's offset after processing all messages should be n
		t.Errorf("last offset is %d, want %d", int(lastOffset.Offset), n)
	}
}

func TestSameKey(t *testing.T) {
	n := 99
	w := newPartition(
		context.Background(),
		&partitionParams{
			Messages: make(chan *kafka.Message, n),
			Processor: func(ctx context.Context, m *kafka.Message) {
				<-ctx.Done()
			},
			Offsets: make(chan<- kafka.TopicPartition, n),
			Done:    make(chan<- *kafka.Message, n),
		})

	m := &kafka.Message{Key: []byte{1}}

	for i := 0; i < n; i++ {
		w.handleMessage(m)
	}

	if w.running != 1 {
		t.Errorf("%d running jobs, want 1", w.running)
	}

	j := w.jobs.Get(newJobKey(m.Key)).(*job)

	if len(j.attached) != 98 {
		t.Errorf("%d attached messages to the job, want %d", len(j.attached), n-1)
	}
}

func TestEmptyKey(t *testing.T) {
	n := 99
	processing := make(chan *kafka.Message, n)
	w := newPartition(
		context.Background(),
		&partitionParams{
			Messages: make(chan *kafka.Message, n),
			Processor: func(ctx context.Context, m *kafka.Message) {
				processing <- m
				<-ctx.Done()
			},
			Offsets: make(chan<- kafka.TopicPartition, n),
			Done:    make(chan<- *kafka.Message, n),
		})

	m := &kafka.Message{Key: nil}

	for i := 0; i < n; i++ {
		w.handleMessage(m)
	}

	messages := readMessages(processing, n, time.Second)

	if w.running != n {
		t.Errorf(".running is %d, want %d", w.running, n)
	}
	if len(messages) != n {
		t.Errorf("%d jobs running, want %d", len(messages), n)
	}
}

func newTestPartitionWorker(n int) *partitionWorker {
	w := newPartition(
		context.Background(),
		&partitionParams{
			Messages:  make(chan *kafka.Message, n),
			Processor: func(ctx context.Context, m *kafka.Message) {},
			Offsets:   make(chan<- kafka.TopicPartition, n),
			Done:      make(chan<- *kafka.Message, n),
		})
	w.jobDone = make(chan *job, n)

	for i := 0; i < n; i++ {
		m := &kafka.Message{}
		m.TopicPartition = kafka.TopicPartition{Offset: kafka.Offset(i)}
		if rand.Float64() < 0.8 {
			m.Key = []byte{byte(rand.Intn(5))}
		}

		w.Messages <- m
	}

	return w
}

func ensureAllMessagesConsumed(w *partitionWorker) {
	for {
		select {
		case m := <-w.Messages:
			w.Messages <- m
		default:
			return
		}
	}
}

func readMessages(messages <-chan *kafka.Message, n int, t time.Duration) []*kafka.Message {
	result := make([]*kafka.Message, 0)

loop:
	for n > 0 {
		select {
		case m := <-messages:
			n -= 1
			result = append(result, m)
		case <-time.After(t):
			break loop
		}
	}

	return result
}
