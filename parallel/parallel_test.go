package parallel

import (
	"context"
	"log"
	"math"
	"math/rand"
	"testing"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func ExampleConsumer() {
	var processor Processor = func(ctx context.Context, m *kafka.Message) {
		defer func() {
			if r := recover(); r != nil {
				// you shouldn't panic in processor function
				// consider recovering, and if nothing can be done,
				// then send message to dead letter queue
			}
		}()
		// ...
	}

	config := ConsumerDefaultConfig
	// set's limits on number and size of messages processed by consumer
	config.MaxMessages = config.MaxMessages
	config.MaxMessagesByte = config.MaxMessagesByte

	kafkaConfig := &kafka.ConfigMap{
		"bootstrap.servers":             "localhost:9092",
		"group.id":                      "example",
		"partition.assignment.strategy": "cooperative-sticky",
		"auto.offset.reset":             "earliest",
		"enable.auto.offset.store":      false,
	}

	ctx, _ := context.WithTimeout(context.Background(), time.Minute)

	c, err := NewConsumer(config, kafkaConfig)
	if err != nil {
		log.Fatal(err)
	}

	err = c.Run(ctx, []string{topic}, processor)
	if err != nil {
		log.Fatal(err)
	}
}

func BenchmarkConsumer(b *testing.B) {
	simple := newSimpleConsumer(b.N)
	consumer, err := consumerFromSimpleConsumer(simple)
	if err != nil {
		b.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	processor := func(ctx context.Context, m *kafka.Message) {}

	go consumer.Run(ctx, []string{}, processor)

	<-simple.Finished
}

func consumerFromSimpleConsumer(s *simpleConsumer) (*Consumer, error) {
	consumerProvider := func(topics []string) (kafkaConsumer, error) {
		return s, nil
	}
	config := &ConsumerDefaultConfig
	config.MaxMessages = math.MaxInt64
	config.MaxMessagesByte = math.MaxInt64

	c, err := newConsumer(consumerProvider, config)
	if err != nil {
		return nil, err
	}

	return c, nil
}

var topic = "topic"

type simpleConsumer struct {
	N              int
	totalProcessed int
	Finished       chan struct{}
	finished       bool

	partitions int

	assigned     bool
	offsetToPoll []int

	offsetStored []int
}

func newSimpleConsumer(n int) *simpleConsumer {
	c := &simpleConsumer{
		N:          n,
		Finished:   make(chan struct{}),
		partitions: 5,
	}
	c.offsetToPoll = make([]int, c.partitions)
	c.offsetStored = make([]int, c.partitions)
	return c
}

func (c *simpleConsumer) Poll(timeoutMs int) (event kafka.Event) {
	if !c.assigned {
		c.assigned = true
		partitions := []kafka.TopicPartition{}
		for i := 0; i < c.partitions; i++ {
			partitions = append(partitions, kafka.TopicPartition{Topic: &topic, Partition: int32(i)})
		}
		return kafka.AssignedPartitions{Partitions: partitions}
	}

	p := int32(rand.Int() % c.partitions)
	o := kafka.Offset(c.offsetToPoll[p])
	partition := kafka.TopicPartition{Topic: &topic, Partition: p, Offset: o}
	k := []byte{byte(rand.Int()), byte(rand.Int()), byte(rand.Int())}

	m := &kafka.Message{TopicPartition: partition, Key: k}

	c.offsetToPoll[p] += 1

	return m
}

func (c *simpleConsumer) StoreOffsets(offsets []kafka.TopicPartition) (storedOffsets []kafka.TopicPartition, err error) {
	d := 0
	for i := range offsets {
		p := int(offsets[i].Partition)
		o := int(offsets[i].Offset)

		d += o - c.offsetStored[p]
		c.offsetStored[p] = o
	}

	c.totalProcessed += d

	if c.totalProcessed >= c.N && !c.finished {
		close(c.Finished)
		c.finished = true
	}

	return nil, nil
}

func (c *simpleConsumer) Assignment() (partitions []kafka.TopicPartition, err error) {
	return []kafka.TopicPartition{}, nil
}

func (c *simpleConsumer) SubscribeTopics(topics []string, rebalanceCb kafka.RebalanceCb) (err error) {
	return nil
}
func (c *simpleConsumer) IncrementalAssign(partitions []kafka.TopicPartition) (err error) {
	return nil
}
func (c *simpleConsumer) IncrementalUnassign(partitions []kafka.TopicPartition) (err error) {
	return nil
}
func (c *simpleConsumer) Unsubscribe() (err error) {
	return nil
}
func (c *simpleConsumer) Close() (err error) {
	return nil
}
