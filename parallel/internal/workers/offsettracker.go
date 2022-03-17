package workers

import "fmt"

// offsetTracker tracks what messages got processed
// and tells when we can move partition's offset on broker.
type offsetTracker struct {
	current int
	done    map[int]struct{}
}

func newOffsetTracker(initialOffset int) *offsetTracker {
	return &offsetTracker{
		current: initialOffset,
		done:    make(map[int]struct{})}
}

// Mark marks message at offset as completed,
// returns either -1 if we can't move offset or new offset.
//
// 		messages:  1 2 3 4 5
// 		processed:     x   x
//
// 		.Mark(2) -> -1      	// we didn't process message at offset=1
// 		messages:  1 2 3 4 5	// so we can't move partition's offset
// 		processed:   x x   x
//
// 		.Mark(1) -> 4       	// and after processing it we can move
// 		messages:  1 2 3 4 5	// partition offset to 4 because
// 		processed: x x x   x	// we already processed messages at offsets 2 and 3
func (t *offsetTracker) Mark(offset int) (move int, err error) {
	if offset < t.current {
		return -1, fmt.Errorf("received an offset that we already counted as processed")
	}
	if t.current != offset {
		t.done[offset] = struct{}{}
		return -1, nil
	}

	newOffset := t.current + 1
	for {
		_, ok := t.done[newOffset]
		if !ok {
			break
		}
		delete(t.done, newOffset)
		newOffset++
	}

	t.current = newOffset
	return newOffset, nil
}
