package limiter

import (
	"sync"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type Limiter struct {
	limits          Limits
	messages, bytes int

	cond sync.Cond
}

type Limits struct {
	MaxMessages int
	MaxBytes    int
}

func New(limits Limits) *Limiter {
	return &Limiter{
		limits: limits,
		cond:   *sync.NewCond(&sync.Mutex{}),
	}
}

func (l *Limiter) Start(input <-chan kafka.Event, processed <-chan *kafka.Message) (output <-chan kafka.Event) {
	out := make(chan kafka.Event)

	go func() {
		for e := range input {
			m, ok := e.(*kafka.Message)
			if ok {
				l.Add(m)
			}

			out <- e

			l.Limit()
		}
		close(out)
	}()

	go func() {
		for m := range processed {
			l.Remove(m)
		}
	}()

	return out
}

func (l *Limiter) Add(m *kafka.Message) {
	l.cond.L.Lock()
	l.messages += 1
	l.bytes += messageSize(m)
	l.cond.L.Unlock()
}

func (l *Limiter) Remove(m *kafka.Message) {
	l.cond.L.Lock()
	l.messages -= 1
	l.bytes -= messageSize(m)
	l.cond.Broadcast()
	l.cond.L.Unlock()
}

// Limit blocks until limits are satisfied.
func (l *Limiter) Limit() {
	l.cond.L.Lock()
	for l.limited() {
		l.cond.Wait()
	}
	l.cond.L.Unlock()
}

func (l *Limiter) limited() bool {
	return l.messages >= l.limits.MaxMessages ||
		l.bytes >= l.limits.MaxBytes
}

func messageSize(m *kafka.Message) int {
	size := len(m.Key) + len(m.Value)
	return size
}
