package workers

import (
	"context"
	"math/rand"
	"testing"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func TestSchedulerRouting(t *testing.T) {
	n := 999
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	events := make(chan kafka.Event, n)
	done := make(chan *kafka.Message, n)
	s := NewScheduler(ctx, func(ctx context.Context, m *kafka.Message) {})
	s.done = done
	s.offsets = make(chan kafka.TopicPartition, n)

	topic, topic2 := "topic", "topic2"
	p1 := kafka.TopicPartition{Topic: &topic, Partition: 1}
	p3 := kafka.TopicPartition{Topic: &topic, Partition: 3}
	s.handleAssign(kafka.AssignedPartitions{Partitions: []kafka.TopicPartition{p1, p3}})

	w1 := s.workers.Get(newTopicPartition(&topic, 1)).(*partitionWorker)
	w1messages := make(chan *kafka.Message, n)
	expected := 0
	w1.processor = func(ctx context.Context, m *kafka.Message) {
		w1messages <- m
	}

	go s.Run(events)

	for i := 0; i < n; i++ {
		var t *string
		if rand.Float64() < 0.5 {
			t = &topic
		} else {
			t = &topic2
		}
		m := &kafka.Message{TopicPartition: kafka.TopicPartition{Topic: t, Partition: int32(i % 10)}}
		if m.TopicPartition.Topic == &topic && m.TopicPartition.Partition == 1 {
			expected += 1
		}

		events <- m
		<-done
	}

	received := 0
	close(w1messages)
	for m := range w1messages {
		received += 1
		if m.TopicPartition.Topic != &topic || m.TopicPartition.Partition != 1 {
			t.Errorf("w1 got message that belong to different partition: %v", m)
		}
	}

	if received != expected {
		t.Errorf("w1 got %d messages, want %d", received, expected)
	}
}

// runPartitionWorker should collect unprocessed messages and send them to .done
func TestRunPartitionWorker(t *testing.T) {
	n := 999
	done := make(chan *kafka.Message, n)
	s := &Scheduler{done: done}

	w := newPartition(
		context.Background(),
		&partitionParams{
			Messages:  make(chan *kafka.Message, n),
			Processor: func(ctx context.Context, m *kafka.Message) {},
			Offsets:   make(chan<- kafka.TopicPartition, n),
			Done:      done,
		})

	for i := 0; i < n; i++ {
		w.Messages <- &kafka.Message{}
	}

	s.wg.Add(1)
	s.startPartitionWorker(w)

	messages := readMessages(done, n, time.Second)
	if len(messages) != n {
		t.Errorf("received %d messages, want %d", len(messages), n)
	}
}

func TestTopicPartition(t *testing.T) {
	name0, name1 := "name", "name"
	t0 := newTopicPartition(&name0, 0)
	t1 := newTopicPartition(&name1, 0)

	if t0.Hash() != t1.Hash() {
		t.Errorf("equal keys has different hashes %d %d, want the same", t0.Hash(), t1.Hash())
	}

	name1 = "different"
	t1 = newTopicPartition(&name1, 0)
	t1.hashCode = t0.hashCode

	if t0.EqualTo(t1) {
		t.Errorf("keys with equal hashCodes but different keys are equal, want not equal")
	}
}
