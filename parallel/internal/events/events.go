package events

import "github.com/mediakovda/go-parallel-consumer/parallel/events"

type Event interface{}

type Message = events.Message
type Header = events.Header

type PartitionsAssigned struct {
	Partitions []TopicPartition
}

type PartitionsRevoked struct {
	Partitions []TopicPartition
}

type OffsetUpdate struct {
	Topic     string
	Partition int
	Offset    int64
}

type TopicPartition struct {
	Topic     string
	Partition int
}
