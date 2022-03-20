package parallel

import (
	"fmt"
	"log"
	"os"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type Config struct {
	PollTimeoutMs int

	Logger *log.Logger
}

// ConsumerDefaultConfig is default config for parallel.Consumer.
//
// See consumer.Config.
var ConsumerDefaultConfig = Config{
	PollTimeoutMs: 100,
	Logger:        log.New(os.Stderr, "", log.LstdFlags),
}

// Verify verifies config.
func (c *Config) Verify() error {
	if c.PollTimeoutMs < 0 {
		return fmt.Errorf("Config.PollTimeoutMs can't be less than 0")
	}

	if c.Logger == nil {
		return fmt.Errorf("Config.Logger can't be nil")
	}

	return nil
}

// VerifyKafkaConfig makes sure kafka.Consumer created from this config
// suited for the task.
//
// Currently we require you to
// disable 'enable.auto.offset.store' and enable 'enable.auto.commit'.
func VerifyKafkaConfig(conf *kafka.ConfigMap) error {
	v, _ := conf.Get("enable.auto.offset.store", nil)
	if v != "false" && v != false {
		return fmt.Errorf("please set 'enable.auto.offset.store' to false")
	}

	v, _ = conf.Get("enable.auto.commit", "true")
	if v != "true" && v != true {
		return fmt.Errorf("please set 'enable.auto.commit' to true")
	}

	return nil
}
