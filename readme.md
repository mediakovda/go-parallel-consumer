# Go Parallel Consumer

```go
func messageProcessor(ctx context.Context, m *kafka.Message) {
    // ...
}

var consumer parallel.Consumer
// ...

go consumer.Run(ctx, topics, messageProcessor)
```

Process concurrently hundreds and thousands messages from topic without regard to the number of partitions.

Group rebalancing is supported.

Checkout demo

```bash
./demo/scripts/demo_setup.sh
./demo/scripts/demo_run.sh
```

or see example in `parallel/consumer_test.go`.

P.S. for now, this is just a small library inspired by interview question. Use with caution.

## How Does It Work?

Consumer reads messages and for each message launches goroutine that processes it (as long as the limits allow and only for assigned partitions).

As the messages get processed, we update offsets: latest offset without skipping any unprocessed messages.

```
v               current offset
0 1 2 3 4 5     message offset
□ □ □ □ □ □     processing messages

v
0 1 2 3 4 5
□ ■ ■ □ ■ □

      v         offset moved after message 0 got processed
0 1 2 3 4 5
■ ■ ■ □ ■ □

          v
0 1 2 3 4 5
■ ■ ■ ■ ■ □
```

Currently, if partition is revoked, all work on uncommited offsets going to be repeated.

```
    v
0 1 2 3 4 5
■ ■ □ ■ □ ■

--- partition revoked ---

    v
0 1 2 3 4 5
■ ■ □ x □ x    another consumer has to repeat work on 3 and 5

```
