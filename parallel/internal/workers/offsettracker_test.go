package workers

import (
	"testing"
)

func TestOffsetTracker(t *testing.T) {
	check := func(offset, got, want int) {
		if got != want {
			t.Errorf("marked %d, got new offset %d, want %d", offset, got, want)
		}
	}

	o := newOffsetTracker(0)

	n, _ := o.Mark(5) // 0 1 2 3 4 5
	check(5, n, -1)   //           x

	n, _ = o.Mark(0) // 0 1 2 3 4 5
	check(0, n, 1)   // x         x

	n, _ = o.Mark(3) // 1 2 3 4 5
	check(3, n, -1)  //     x   x

	n, _ = o.Mark(2) // 1 2 3 4 5
	check(2, n, -1)  //   x x   x

	n, _ = o.Mark(1) // 1 2 3 4 5
	check(1, n, 4)   // x x x   x

	n, _ = o.Mark(4) // 4 5
	check(4, n, 6)   // x x

	if len(o.done) != 0 {
		t.Error("there are some items left in map")
	}
}
