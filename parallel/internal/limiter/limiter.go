package limiter

import (
	"sync"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type Limiter struct {
	maxMessages, maxBytes int
	messages, bytes       int

	cond sync.Cond
}

func New(maxMessages, maxMessagesBytes int) *Limiter {
	return &Limiter{
		maxMessages: maxMessages,
		maxBytes:    maxMessagesBytes,
		cond:        *sync.NewCond(&sync.Mutex{}),
	}
}

func (l *Limiter) Add(m *kafka.Message) {
	l.cond.L.Lock()
	l.messages += 1
	l.bytes += len(m.Key) + len(m.Value)
	l.cond.L.Unlock()
}

func (l *Limiter) Remove(m *kafka.Message) {
	l.cond.L.Lock()
	l.messages -= 1
	l.bytes -= len(m.Key) + len(m.Value)
	l.cond.Broadcast()
	l.cond.L.Unlock()
}

// Limit blocks until limits are satisfied.
func (l *Limiter) Limit() {
	l.cond.L.Lock()
	for l.messages > l.maxMessages || l.bytes > l.maxBytes {
		l.cond.Wait()
	}
	l.cond.L.Unlock()
}
