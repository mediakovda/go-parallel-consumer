package events

// Message represents message received from Kafka
type Message struct {
	Topic     string
	Partition int
	Offset    int64

	Key     []byte
	Value   []byte
	Headers []Header
}

// Header is Kafka message's header
type Header struct {
	Key   string
	Value []byte
}
