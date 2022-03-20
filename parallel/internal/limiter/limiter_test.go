package limiter

import (
	"math"
	"testing"
	"time"

	"github.com/mediakovda/go-parallel-consumer/parallel/internal/events"
)

func TestLimiterMessages(t *testing.T) {
	l := New(Limits{MaxMessages: 1, MaxBytes: math.MaxInt64})
	src := make(chan events.Event, 2)
	src <- &events.Message{}
	src <- &events.Message{}
	processed := make(chan *events.Message)

	out := l.Start(src, processed)

	<-out

	if !l.limited() {
		t.Errorf("not limited(), want limited()")
	}

	processed <- &events.Message{}

	select {
	case <-out:

	case <-time.After(time.Second):
		t.Errorf("no message, want one")
	}
}

func TestLimiterMessageBytes(t *testing.T) {
	l := New(Limits{MaxMessages: math.MaxInt64, MaxBytes: 1})

	src := make(chan events.Event, 2)
	src <- &events.Message{Value: []byte{0}}
	src <- &events.Message{Value: []byte{0}}
	processed := make(chan *events.Message)

	out := l.Start(src, processed)

	<-out

	if !l.limited() {
		t.Errorf("not limited(), want limited()")
	}

	processed <- &events.Message{Value: []byte{0}}

	select {
	case <-out:

	case <-time.After(time.Second):
		t.Errorf("no message, want one")
	}
}

func TestMessageSize(t *testing.T) {
	m := &events.Message{
		Headers: []events.Header{{Key: "h", Value: make([]byte, 10)}},
		Key:     make([]byte, 100),
		Value:   make([]byte, 1000),
	}

	size := messageSize(m)
	if size != 1111 {
		t.Errorf("message size = %d, want 1111", size)
	}

	m = &events.Message{}

	size = messageSize(m)
	if size != 0 {
		t.Errorf("message size = %d, want 0", size)
	}
}
