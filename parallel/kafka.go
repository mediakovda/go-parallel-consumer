package parallel

import (
	"context"
	"log"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type kafkaPoller struct {
	events   chan kafka.Event
	offsets  chan kafka.TopicPartition
	consumer kafkaConsumer
	context  context.Context

	logger        *log.Logger
	pollTimeoutMs int
}

type kafkaConsumer interface {
	SubscribeTopics(topics []string, rebalanceCb kafka.RebalanceCb) (err error)
	Poll(timeoutMs int) (event kafka.Event)
	IncrementalAssign(partitions []kafka.TopicPartition) (err error)
	IncrementalUnassign(partitions []kafka.TopicPartition) (err error)
	StoreOffsets(offsets []kafka.TopicPartition) (storedOffsets []kafka.TopicPartition, err error)
	Close() (err error)
}

func newKafkaPoller(ctx context.Context, c kafkaConsumer, topics []string, config *Config) (*kafkaPoller, error) {
	p := &kafkaPoller{
		events:        make(chan kafka.Event),
		offsets:       make(chan kafka.TopicPartition),
		consumer:      c,
		context:       ctx,
		logger:        config.Logger,
		pollTimeoutMs: config.PollTimeoutMs,
	}

	err := c.SubscribeTopics(topics, p.rebalance)
	if err != nil {
		return nil, err
	}

	return p, nil
}

func (p *kafkaPoller) Events() <-chan kafka.Event {
	return p.events
}

func (p *kafkaPoller) Offsets() chan<- kafka.TopicPartition {
	return p.offsets
}

func (p *kafkaPoller) Run() {
	go p.offsetLoop()

loop:
	for {
		select {
		case <-p.context.Done():
			break loop

		default:
			e := p.consumer.Poll(p.pollTimeoutMs)
			p.handleEvent(e)
		}
	}

	p.consumer.Close()
	close(p.events)
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
		select {
		case p.events <- send:
		case <-p.context.Done():
		}
	}
}

func (p *kafkaPoller) offsetLoop() {
	for {
		select {
		case o := <-p.offsets:
			_, err := p.consumer.StoreOffsets([]kafka.TopicPartition{o})

			if err != nil {
				p.logger.Printf("offsetLoop, consumer.StoreOffsets: %v\n", err)
			}

		case <-p.context.Done():
			return
		}
	}
}
