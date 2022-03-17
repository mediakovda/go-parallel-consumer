/*
[work in progress]

Package consumer provides parallel.Consumer that lets you
process any amount of messages concurrently regardless of
number of partitions.

You can set limit for number of messages or total size
of messages being processed.

Messages with the same keys processed sequentially.

As messages gets processed, progress is stored on broker.

Group rebalancing is supported.
*/
package parallel

import (
	"context"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/mediakovda/go-parallel-consumer/parallel/internal/limiter"
	"github.com/mediakovda/go-parallel-consumer/parallel/internal/workers"
)

// Processor is the function that will be used to process the messages.
//
// It shouldn't panic. Or we'll panic back.
type Processor = func(ctx context.Context, m *kafka.Message)

// Consumer provides easy way to process partitions with key concurrency.
type Consumer struct {
	config *Config

	kconsumer kconsumerProvider
}

type kconsumerProvider func(topics []string) (kafkaConsumer, error)

// NewConsumer checks configs and creates new parallel.Consumer.
func NewConsumer(config Config, kafkaConfig *kafka.ConfigMap) (*Consumer, error) {
	err := config.Verify()
	if err != nil {
		return nil, err
	}

	err = VerifyKafkaConfig(kafkaConfig)
	if err != nil {
		return nil, err
	}

	consumer := func(topics []string) (kafkaConsumer, error) {
		kc, err := kafka.NewConsumer(kafkaConfig)
		if err != nil {
			return nil, err
		}
		return kc, nil
	}

	p, err := newConsumer(consumer, &config)
	if err != nil {
		return nil, err
	}

	return p, err
}

func newConsumer(consumer kconsumerProvider, conf *Config) (*Consumer, error) {
	p := &Consumer{
		config:    conf,
		kconsumer: consumer,
	}

	return p, nil
}

// Run starts reading messages from topics and processing them.
//
// Blocks until ctx is done.
func (c *Consumer) Run(ctx context.Context, topics []string, f Processor) error {
	kc, err := c.kconsumer(topics)
	if err != nil {
		return err
	}

	poller, err := newKafkaPoller(ctx, kc, topics, c.config)
	if err != nil {
		return err
	}

	scheduler := workers.NewScheduler(ctx, f)

	finished := make(chan struct{}, 2)
	go func() {
		poller.Run()
		finished <- struct{}{}
	}()

	events := poller.Events()
	events = limit(ctx, events, scheduler.Done(), c.config.MaxMessages, c.config.MaxMessagesByte)

	go func() {
		scheduler.Run(events)
		finished <- struct{}{}
	}()

	go pipeOffsets(ctx, scheduler.Offsets(), poller.Offsets())

	<-finished
	<-finished

	return nil
}

func pipeOffsets(ctx context.Context, in <-chan kafka.TopicPartition, out chan<- kafka.TopicPartition) {
	for o := range in {
		select {
		case out <- o:
		case <-ctx.Done():
		}
	}
}

func limit(ctx context.Context, input <-chan kafka.Event, done <-chan *kafka.Message, countLimit, sizeLimit int) (output <-chan kafka.Event) {
	out := make(chan kafka.Event)
	l := limiter.New(countLimit, sizeLimit)

	go func() {
		for m := range done {
			l.Remove(m)
		}
	}()

	go func() {
		for e := range input {
			m, ok := e.(*kafka.Message)
			if ok {
				l.Add(m)
			}

			select {
			case out <- e:
			case <-ctx.Done():
			}

			l.Limit()
		}
	}()

	return out
}
