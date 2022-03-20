package workers

import (
	"context"
	"testing"

	"github.com/mediakovda/go-parallel-consumer/parallel/internal/events"
)

func TestJobRun(t *testing.T) {
	m := &events.Message{}
	ctx, cancel := context.WithCancel(context.Background())

	j := newJob(nil, m)
	j.run(ctx, func(ctx context.Context, m *events.Message) {})
	if j.err != nil {
		t.Errorf("job failed, want not failed")
	}

	j = newJob(nil, m)
	j.run(ctx, func(ctx context.Context, m *events.Message) {
		panic("")
	})
	if j.err == nil {
		t.Errorf("job not failed, want failed")
	}

	cancel()
	j = newJob(nil, m)
	j.run(ctx, func(ctx context.Context, m *events.Message) {})
	if j.err == nil {
		t.Errorf("job not failed, want failed")
	}

	j = newJob(nil, nil)
	j.run(ctx, func(ctx context.Context, m *events.Message) {})
	if j.err == nil {
		t.Errorf("job with .current = nil isn't failed, want failed")
	}
}

func TestNext(t *testing.T) {
	m0 := &events.Message{}
	m1 := &events.Message{}

	j := newJob(nil, m0)
	j.run(context.Background(), func(ctx context.Context, m *events.Message) {
		panic("")
	})

	j.attach(m1)

	j = j.next()
	if j.current != m1 {
		t.Errorf("new job's current message is %v, want %v", j.current, m1)
	}
	if j.err != nil {
		t.Errorf("new job is failed, want not failed")
	}

	j = j.next()
	if j != nil {
		t.Errorf("new job is %v, want nil", j)
	}
}

func TestJobKey(t *testing.T) {
	k0 := newJobKey([]byte{64})
	k1 := newJobKey([]byte{64})

	if k0.Hash() != k1.Hash() {
		t.Errorf("equal keys has different hashes %d %d, want the same", k0.Hash(), k1.Hash())
	}

	k1 = newJobKey([]byte{128})
	k1.hashCode = k0.hashCode

	if k0.EqualTo(k1) {
		t.Errorf("keys with equal hashCodes but different keys are equal, want not equal")
	}
}
