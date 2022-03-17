package workers

import (
	"context"
	"hash/crc32"
	"mediakov/parcon/parallel/internal/hashmap"
	"sync"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type Processor = func(ctx context.Context, m *kafka.Message)

// Scheduler creates and closes partitionWorkers and
// routes messages to them
type Scheduler struct {
	context   context.Context
	processor Processor
	offsets   chan kafka.TopicPartition
	done      chan *kafka.Message

	workers *hashmap.HashMap // map[*topicPartition]*partitionWorker
	wg      sync.WaitGroup
}

func NewScheduler(ctx context.Context, f Processor) *Scheduler {
	s := &Scheduler{
		context:   ctx,
		processor: f,
		offsets:   make(chan kafka.TopicPartition),
		done:      make(chan *kafka.Message),
		workers:   hashmap.New(),
	}

	return s
}

func (s *Scheduler) Offsets() <-chan kafka.TopicPartition {
	return s.offsets
}

// Done returns channel with messages that are completely finished processing.
func (s *Scheduler) Done() <-chan *kafka.Message {
	return s.done
}

func (s *Scheduler) Run(events <-chan kafka.Event) {

loop:
	for {
		select {
		case event := <-events:
			switch e := event.(type) {
			case kafka.AssignedPartitions:
				s.handleAssign(e)
			case kafka.RevokedPartitions:
				s.handleRevoke(e)
			case *kafka.Message:
				s.handleMessage(e)
			default:
			}

		case <-s.context.Done():
			break loop
		}
	}

	for _, v := range s.workers.List() {
		w := v.(*partitionWorker)
		s.stopPartitionWorker(w)
	}

	s.wg.Wait()

	close(s.done)
	close(s.offsets)
}

func (s *Scheduler) handleAssign(e kafka.AssignedPartitions) {
	for _, p := range e.Partitions {
		k := newTopicPartition(p.Topic, p.Partition)

		if s.workers.Get(k) != nil {
			continue
		}

		w := newPartition(
			s.context,
			&partitionParams{
				Messages:  make(chan *kafka.Message),
				Processor: s.processor,
				Offsets:   s.offsets,
				Done:      s.done,
			})
		s.workers.Put(k, w)

		s.startPartitionWorker(w)
	}
}

func (s *Scheduler) handleRevoke(e kafka.RevokedPartitions) {
	for _, p := range e.Partitions {
		k := newTopicPartition(p.Topic, p.Partition)
		v := s.workers.Pop(k)
		if v == nil {
			continue
		}

		w := v.(*partitionWorker)
		s.stopPartitionWorker(w)
	}
}

func (s *Scheduler) handleMessage(m *kafka.Message) {
	k := newTopicPartition(m.TopicPartition.Topic, m.TopicPartition.Partition)
	v := s.workers.Get(k)
	if v == nil {
		s.done <- m
		return
	}

	w := v.(*partitionWorker)
	w.Messages <- m
}

func (s *Scheduler) startPartitionWorker(w *partitionWorker) {
	s.wg.Add(1)
	go func() {
		w.Run()
		s.wg.Done()
	}()
}

func (s *Scheduler) stopPartitionWorker(w *partitionWorker) {
	w.Close()
}

type topicPartition struct {
	topic     *string
	partition int32
	hashCode  int
}

func newTopicPartition(topic *string, partition int32) *topicPartition {
	return &topicPartition{
		topic:     topic,
		partition: partition,
	}
}

func (k *topicPartition) Hash() int {
	if k.hashCode != 0 {
		return k.hashCode
	}

	var h int = 7

	h = 31*h + int(crc32.ChecksumIEEE([]byte(*k.topic)))
	h = 31*h + int(k.partition)

	if h == 0 {
		h = 1
	}

	k.hashCode = h
	return h
}

func (k *topicPartition) EqualTo(other interface{}) bool {
	switch o := other.(type) {
	case *topicPartition:
		if k.Hash() != o.Hash() {
			return false
		}
		return k.partition == o.partition && *k.topic == *o.topic
	default:
		return false
	}
}
