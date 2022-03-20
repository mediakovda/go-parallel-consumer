package main

import (
	"context"
	"flag"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"sync/atomic"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/mediakovda/go-parallel-consumer/parallel"
)

var (
	bootstrap = "localhost:9092"
	topic     = "example"
)

var (
	shouldConsume = false
	groupId       = "example"
	pdelay        = int(time.Second / time.Millisecond)
	maxMessages   = 100
	maxBytes      = 100 * 1024 * 1024
)

var (
	shouldProduce   = false
	produceMessages = 1000
	keySize         = 5
	emptyKeyRatio   = 0.2
	valueSize       = 150
)

func main() {
	flag.StringVar(&bootstrap, "bootstrap", bootstrap, "bootstrap server address")
	flag.StringVar(&topic, "topic", topic, "topic name")

	flag.BoolVar(&shouldConsume, "consume", shouldConsume, "should we consume some messages?")
	flag.StringVar(&groupId, "groupid", groupId, "consumer group id")
	flag.IntVar(&pdelay, "pdelay", pdelay, "how long one message will be processed, milliseconds")
	flag.IntVar(&maxMessages, "max", maxMessages, "maximum amount of messages currently processed by consumer")
	flag.IntVar(&maxBytes, "maxbytes", maxBytes, "maximum size in bytes of all messages currently processed by consumer")

	flag.BoolVar(&shouldProduce, "produce", shouldProduce, "should we produce some messages?")
	flag.IntVar(&produceMessages, "n", produceMessages, "number of messages to produce")
	flag.IntVar(&keySize, "key", keySize, "key size in bytes")
	emptyKeyPercentage := int(100.0 * emptyKeyRatio)
	flag.IntVar(&emptyKeyPercentage, "empty", emptyKeyPercentage, "percentage of messages with empty key")
	flag.IntVar(&valueSize, "value", valueSize, "size of messages' value in bytes")

	flag.Parse()
	emptyKeyRatio = float64(emptyKeyPercentage) / 100.0

	validateFlags()

	if shouldProduce {
		produce()
	}

	if shouldConsume {
		consume()
	}
}

func consume() {
	log.Printf("bootstrap %s, topic %s", bootstrap, topic)
	log.Printf("limits: amount of messages: %d, size of messages: %d bytes", maxMessages, maxBytes)
	log.Printf("time for processing of one message: %d ms", pdelay)

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		<-exit()
		cancel()
	}()

	var messages int64
	go speedPrinter(ctx, &messages)

	processor := func(ctx context.Context, m *kafka.Message) {
		select {
		case <-time.After(time.Duration(pdelay) * time.Millisecond):
		case <-ctx.Done():
		}
		atomic.AddInt64(&messages, 1)
	}

	config := parallel.ConsumerDefaultConfig
	limiter := parallel.NewLimiter(parallel.Limits{
		MaxMessages: 100,
		MaxBytes:    100 * 1024 * 1024,
	})

	kafkaConfig := &kafka.ConfigMap{
		"bootstrap.servers":             bootstrap,
		"group.id":                      groupId,
		"partition.assignment.strategy": "cooperative-sticky",
		"auto.offset.reset":             "earliest",
		"enable.auto.offset.store":      false,
	}

	c, err := parallel.NewConsumer(config, kafkaConfig)
	if err != nil {
		log.Fatal(err)
	}

	err = c.Run(ctx, []string{topic}, processor, limiter)
	if err != nil {
		log.Fatal(err)
	}
}

func speedPrinter(ctx context.Context, messages *int64) {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	nothing := false
	start := time.Now()
loop:
	for {
		select {
		case <-ticker.C:
			n := atomic.SwapInt64(messages, 0)
			now := time.Now()
			d := now.Sub(start)

			if !nothing {
				speed := float64(n) / (float64(d) / float64(time.Second))
				log.Printf("%.1f messages per second", speed)
			}

			start = now
			nothing = n == 0

		case <-ctx.Done():
			break loop
		}
	}
}

func produce() {
	log.Printf("bootstrap %s, topic %s", bootstrap, topic)
	log.Printf("producing %d message with key size %d bytes, %.0f%% of empty key, value size %d bytes", produceMessages, keySize, 100*emptyKeyRatio, valueSize)

	p, _ := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers":      bootstrap,
		"go.logs.channel.enable": true,
	})

	go func() {
		for l := range p.Logs() {
			log.Println(l)
		}
	}()

	proceed := true
	go func() {
		<-exit()
		proceed = false
	}()

	for i := 0; i < produceMessages && proceed; i++ {
		var key []byte
		if 1-emptyKeyRatio < rand.Float64() {
			key = make([]byte, keySize)
			rand.Read(key)
		}

		value := make([]byte, valueSize)
		rand.Read(value)

		err := p.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
			Key:            key,
			Value:          value,
		}, nil)
		if err != nil {
			log.Println(err)

			e, ok := err.(kafka.Error)
			if !ok {
				continue
			}

			switch e.Code() {
			case kafka.ErrQueueFull:
				p.Flush(1000)
				i--
			default:
			}
		}
	}

	p.Flush(1000)
	p.Close()
}

func validateFlags() {
	ok := true

	if shouldConsume {
		if maxMessages < 1 {
			log.Println("max: can't be less than 1")
			ok = false
		}
		if maxBytes < 1 {
			log.Println("maxBytes: can't be less than 1")
			ok = false
		}
	}

	if shouldProduce {
		if produceMessages < 0 {
			log.Println("n: should be equal or greater than 0")
			ok = false
		}
		if keySize < 0 {
			log.Println("key: key size should be equal or greater than 0")
			ok = false
		}
		if emptyKeyRatio < 0 || emptyKeyRatio > 1 {
			log.Println("empty: percentage of empty messages' keys should be in range from 0 to 100")
			ok = false
		}
		if valueSize < 0 {
			log.Println("value: message's value size should be equal or greater than 0")
			ok = false
		}
	}

	if !ok {
		os.Exit(1)
	}
}

func exit() chan struct{} {
	done := make(chan struct{})
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	go func() {
		for range c {
			close(done)
		}
	}()
	return done
}
