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
	"fmt"
	"sync/atomic"

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

	runned int32
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
// Should be called only once. Blocks until ctx is done.
func (c *Consumer) Run(ctx context.Context, topics []string, f Processor) error {
	runned := atomic.SwapInt32(&c.runned, 1)
	if runned == 1 {
		return fmt.Errorf("Consumer.Run should be called only once")
	}

	//                           ┌─────┐
	//  ┌────────┐ ---events---> │limit│ ---events---> ┌─────────┐
	//  │Consumer│               └─────┘ <--processed- │scheduler│
	//  └────────┘ <------------------------offsets--- └─────────┘

	kc, err := c.kconsumer(topics)
	if err != nil {
		return err
	}
	poller, err := newKafkaPoller(kc, topics, c.config)
	if err != nil {
		return err
	}
	pollerStopped := make(chan error, 1)
	go func() {
		err := poller.Run(ctx)
		if err != nil {
			pollerStopped <- err
		}
		close(pollerStopped)
	}()

	scheduler := workers.NewScheduler(ctx, f)

	limit := limiter.New(limiter.Limits{MaxMessages: c.config.MaxMessages, MaxBytes: c.config.MaxMessagesByte})
	events := poller.Events()
	events = limit.Start(events, scheduler.Processed())

	schedulerStopped := make(chan struct{})
	go func() {
		scheduler.Run(events)
		close(schedulerStopped)
	}()

	offsetsStopped := make(chan struct{})
	go func() {
		poller.RunOffsets(scheduler.Offsets())
		close(offsetsStopped)
	}()

	<-pollerStopped
	err = kc.Close()
	<-schedulerStopped
	<-offsetsStopped

	return err
}
