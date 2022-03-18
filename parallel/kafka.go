package parallel

import (
	"context"
	"log"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type kafkaPoller struct {
	events   chan kafka.Event
	consumer kafkaConsumer
	topics   []string

	logger        *log.Logger
	pollTimeoutMs int
}

type kafkaConsumer interface {
	SubscribeTopics(topics []string, rebalanceCb kafka.RebalanceCb) (err error)
	Poll(timeoutMs int) (event kafka.Event)
	IncrementalAssign(partitions []kafka.TopicPartition) (err error)
	IncrementalUnassign(partitions []kafka.TopicPartition) (err error)
	StoreOffsets(offsets []kafka.TopicPartition) (storedOffsets []kafka.TopicPartition, err error)
	Assignment() (partitions []kafka.TopicPartition, err error)
	Unsubscribe() (err error)
	Close() (err error)
}

func newKafkaPoller(c kafkaConsumer, topics []string, config *Config) (*kafkaPoller, error) {
	p := &kafkaPoller{
		events:        make(chan kafka.Event),
		consumer:      c,
		topics:        topics,
		logger:        config.Logger,
		pollTimeoutMs: config.PollTimeoutMs,
	}

	return p, nil
}

func (p *kafkaPoller) Events() <-chan kafka.Event {
	return p.events
}

func (p *kafkaPoller) Run(ctx context.Context) error {
	err := p.consumer.SubscribeTopics(p.topics, p.rebalance)
	if err != nil {
		return err
	}

	pollDone := p.startPollLoop(ctx)

	<-ctx.Done()
	p.consumer.Unsubscribe()
	err = <-pollDone
	close(p.events)

	return err
}

func (p *kafkaPoller) startPollLoop(ctx context.Context) <-chan error {
	done := make(chan error, 1)

	go func() {
	loop:
		for {
			e := p.consumer.Poll(p.pollTimeoutMs)
			p.handleEvent(e)

			select {
			case <-ctx.Done():
				partitions, err := p.consumer.Assignment()
				if err != nil {
					done <- err
					break loop
				}
				if len(partitions) == 0 {
					break loop
				}

			default:
			}
		}

		close(done)
	}()

	return done
}

func (p *kafkaPoller) rebalance(c *kafka.Consumer, e kafka.Event) error {
	// rebalance only called as a result of calling p.consumer.Poll in pollLoop
	// and there are no concurrent calls to p.handleEvent
	// between rebalance and pollLoop
	p.handleEvent(e)
	return nil
}

func (p *kafkaPoller) handleEvent(e kafka.Event) {
	var send kafka.Event

	switch e := e.(type) {
	case *kafka.Message:
		send = e

	case kafka.Error:
		p.logger.Println("kafka.Error", e)

	case kafka.AssignedPartitions:
		err := p.consumer.IncrementalAssign(e.Partitions)
		if err != nil {
			p.logger.Println("kafka.AssignedPartitions", err)
		}
		send = e
	case kafka.RevokedPartitions:
		err := p.consumer.IncrementalUnassign(e.Partitions)
		if err != nil {
			p.logger.Println("kafka.RevokedPartitions", err)
		}
		send = e

	default:
	}

	if send != nil {
		p.events <- send
	}
}

func (p *kafkaPoller) RunOffsets(offsets <-chan kafka.TopicPartition) {
	for o := range offsets {
		_, err := p.consumer.StoreOffsets([]kafka.TopicPartition{o})

		if err != nil {
			p.logger.Printf("offsetLoop, consumer.StoreOffsets: %v\n", err)
		}
	}
}
