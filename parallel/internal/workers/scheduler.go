package workers

import (
	"context"
	"hash/crc32"
	"sync"

	"github.com/mediakovda/go-parallel-consumer/parallel/internal/events"
	"github.com/mediakovda/go-parallel-consumer/parallel/internal/hashmap"
)

type Processor = func(ctx context.Context, m *events.Message)

// Scheduler creates and closes partitionWorkers and
// routes messages to them
type Scheduler struct {
	context   context.Context
	processor Processor
	offsets   chan events.OffsetUpdate
	done      chan *events.Message

	workers *hashmap.HashMap // map[*topicPartition]*partitionWorker
	wg      sync.WaitGroup
}

func NewScheduler(ctx context.Context, f Processor) *Scheduler {
	s := &Scheduler{
		context:   ctx,
		processor: f,
		offsets:   make(chan events.OffsetUpdate),
		done:      make(chan *events.Message),
		workers:   hashmap.New(),
	}

	return s
}

func (s *Scheduler) Offsets() <-chan events.OffsetUpdate {
	return s.offsets
}

// Processed returns channel with messages that are completely finished processing.
func (s *Scheduler) Processed() <-chan *events.Message {
	return s.done
}

func (s *Scheduler) Run(src <-chan events.Event) {
	for event := range src {
		switch e := event.(type) {
		case events.PartitionsAssigned:
			s.handleAssign(e)
		case events.PartitionsRevoked:
			s.handleRevoke(e)
		case *events.Message:
			s.handleMessage(e)
		default:
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

func (s *Scheduler) handleAssign(e events.PartitionsAssigned) {
	for _, p := range e.Partitions {
		k := newTopicPartition(p.Topic, p.Partition)

		if s.workers.Get(k) != nil {
			continue
		}

		w := newPartition(
			s.context,
			&partitionParams{
				Messages:  make(chan *events.Message),
				Processor: s.processor,
				Offsets:   s.offsets,
				Done:      s.done,
			})
		s.workers.Put(k, w)

		s.startPartitionWorker(w)
	}
}

func (s *Scheduler) handleRevoke(e events.PartitionsRevoked) {
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

func (s *Scheduler) handleMessage(m *events.Message) {
	k := newTopicPartition(m.Topic, m.Partition)
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
	topic     string
	partition int
	hashCode  int
}

func newTopicPartition(topic string, partition int) *topicPartition {
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

	h = 31*h + int(crc32.ChecksumIEEE([]byte(k.topic)))
	h = 31*h + k.partition

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
		return k.partition == o.partition && k.topic == o.topic
	default:
		return false
	}
}
