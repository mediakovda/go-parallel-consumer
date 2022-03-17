package workers

import (
	"bytes"
	"context"
	"fmt"
	"hash/crc32"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

// job is auxiliary structure for partitionWorker.
type job struct {
	key      *jobKey
	current  *kafka.Message
	err      error
	attached []*kafka.Message
}

func newJob(k *jobKey, m *kafka.Message) *job {
	return &job{
		key:     k,
		current: m,
	}
}

func (j *job) attach(m *kafka.Message) {
	j.attached = append(j.attached, m)
}

func (j *job) next() *job {
	if len(j.attached) == 0 {
		return nil
	}

	return &job{
		key:      j.key,
		current:  j.attached[0],
		attached: j.attached[1:],
	}
}

func (j *job) run(ctx context.Context, f Processor) {
	if j.current == nil {
		j.err = fmt.Errorf("job can't have nil current message")
		return
	}

	defer func() {
		if r := recover(); r != nil {
			j.err = fmt.Errorf("processor panicked on message %v: %v", j.current, r)
			return
		}

		select {
		case <-ctx.Done():
			j.err = ctx.Err()
		default:
		}
	}()

	f(ctx, j.current)
}

type jobKey struct {
	key      []byte
	hashCode int
}

func newJobKey(k []byte) *jobKey {
	return &jobKey{
		key: k,
	}
}

func (k *jobKey) IsEmpty() bool {
	return len(k.key) == 0
}

func (k *jobKey) Hash() int {
	if k.hashCode != 0 {
		return k.hashCode
	}

	h := int(crc32.ChecksumIEEE(k.key))
	if h == 0 {
		h = 1
	}

	k.hashCode = h
	return h
}

func (k *jobKey) EqualTo(other interface{}) bool {
	switch o := other.(type) {
	case *jobKey:
		if len(k.key) != len(o.key) || k.Hash() != o.Hash() {
			return false
		}
		return bytes.Equal(k.key, o.key)

	default:
		return false
	}
}
