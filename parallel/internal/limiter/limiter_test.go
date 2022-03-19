package limiter

import (
	"math"
	"testing"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func TestLimiterMessages(t *testing.T) {
	l := New(Limits{MaxMessages: 1, MaxBytes: math.MaxInt64})
	events := make(chan kafka.Event, 2)
	events <- &kafka.Message{}
	events <- &kafka.Message{}
	processed := make(chan *kafka.Message)

	out := l.Start(events, processed)

	<-out

	if !l.limited() {
		t.Errorf("not limited(), want limited()")
	}

	processed <- &kafka.Message{}

	select {
	case <-out:

	case <-time.After(time.Second):
		t.Errorf("no message, want one")
	}
}

func TestLimiterMessageBytes(t *testing.T) {
	l := New(Limits{MaxMessages: math.MaxInt64, MaxBytes: 1})

	events := make(chan kafka.Event, 2)
	events <- &kafka.Message{Value: []byte{0}}
	events <- &kafka.Message{Value: []byte{0}}
	processed := make(chan *kafka.Message)

	out := l.Start(events, processed)

	<-out

	if !l.limited() {
		t.Errorf("not limited(), want limited()")
	}

	processed <- &kafka.Message{Value: []byte{0}}

	select {
	case <-out:

	case <-time.After(time.Second):
		t.Errorf("no message, want one")
	}
}

func TestMessageSize(t *testing.T) {
	m := &kafka.Message{
		Headers: []kafka.Header{{Key: "h", Value: make([]byte, 10)}},
		Key:     make([]byte, 100),
		Value:   make([]byte, 1000),
	}

	size := messageSize(m)
	if size != 1111 {
		t.Errorf("message size = %d, want 1111", size)
	}

	m = &kafka.Message{}

	size = messageSize(m)
	if size != 0 {
		t.Errorf("message size = %d, want 0", size)
	}
}
