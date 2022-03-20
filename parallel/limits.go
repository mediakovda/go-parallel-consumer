package parallel

import (
	"math"

	"github.com/mediakovda/go-parallel-consumer/parallel/internal/events"
	"github.com/mediakovda/go-parallel-consumer/parallel/internal/limiter"
)

type Limits struct {
	// MaxMessages is the maximum number of messages in memory that are not finished processing.
	MaxMessages int

	// MaxBytes is the maximum size of messages in memory that are not finished processing.
	MaxBytes int
}

var NoLimits = Limits{
	MaxMessages: math.MaxInt64,
	MaxBytes:    math.MaxInt64,
}

type Limiter struct {
	limiter *limiter.Limiter
}

func NewLimiter(limits Limits) Limiter {
	return Limiter{
		limiter: limiter.New(limiter.Limits{
			MaxMessages: limits.MaxMessages,
			MaxBytes:    limits.MaxBytes,
		}),
	}
}

func (l Limiter) SetLimits(limits Limits) {
	l.limiter.SetLimits(limiter.Limits{
		MaxMessages: limits.MaxMessages,
		MaxBytes:    limits.MaxBytes,
	})
}

func (l Limiter) limit(input <-chan events.Event, processed <-chan *events.Message) (output <-chan events.Event) {
	return l.limiter.Start(input, processed)
}
