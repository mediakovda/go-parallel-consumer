package hashmap

import "testing"

type item struct {
	i int
}

func (e *item) Hash() int { return e.i % 10 }

func (e *item) EqualTo(other interface{}) bool {
	switch o := other.(type) {
	case *item:
		return o.i == e.i
	default:
		return false
	}
}

func TestPut(t *testing.T) {
	s := New()
	e := &item{1}

	s.Put(e, e)
	s.Put(e, e)

	if len(s.m[1]) != 1 {
		t.Errorf("%d items with hash 1, want 1", len(s.m[1]))
	}

	sameHashItem := &item{11}
	s.Put(sameHashItem, sameHashItem)

	if len(s.m[1]) != 2 {
		t.Errorf("%d items with hash 1, want 2", len(s.m[1]))
	}
}

func TestGet(t *testing.T) {
	s := New()
	a := &item{1}
	b := &item{1}

	s.Put(a, a)

	shouldBeA := s.Get(b)

	if a != shouldBeA {
		t.Errorf("got wrong item from set")
	}

	sameHashItem := &item{11}

	shouldBeNothing := s.Get(sameHashItem)
	if shouldBeNothing != nil {
		t.Errorf("got item %v, want none", shouldBeNothing)
	}
}

func TestPop(t *testing.T) {
	s := New()
	s.Put(&item{1}, &item{1})
	s.Put(&item{11}, &item{11})
	s.Put(&item{21}, &item{21})

	check := func(i, want int) {
		if i != want {
			t.Errorf("%d items with hash 1, want %d", i, want)
		}
	}

	check(len(s.m[1]), 3)
	s.Pop(&item{21})
	check(len(s.m[1]), 2)
	s.Pop(&item{1})
	check(len(s.m[1]), 1)
	s.Pop(&item{11})
	check(len(s.m[1]), 0)
}
