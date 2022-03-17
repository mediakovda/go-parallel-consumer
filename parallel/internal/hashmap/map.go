package hashmap

type HashMap struct {
	m   map[int][]*entry
	len int
}

type MapKey interface {
	Hash() int
	EqualTo(interface{}) bool
}

type entry struct {
	key   MapKey
	value interface{}
}

func New() *HashMap {
	return &HashMap{
		m: make(map[int][]*entry),
	}
}

func (m *HashMap) Put(k MapKey, v interface{}) {
	h := k.Hash()
	l, ok := m.m[h]

	if ok {
		for i := range l {
			if k.EqualTo(l[i].key) {
				l[i].value = v
				return
			}
		}
	}

	m.m[h] = append(l, &entry{key: k, value: v})
	m.len += 1
}

func (m *HashMap) Get(k MapKey) interface{} {
	h := k.Hash()
	l, ok := m.m[h]
	if !ok {
		return nil
	}

	for i := range l {
		if k.EqualTo(l[i].key) {
			return l[i].value
		}
	}

	return nil
}

func (m *HashMap) Pop(k MapKey) interface{} {
	h := k.Hash()
	l, ok := m.m[h]
	if !ok {
		return nil
	}

	for i := range l {
		if k.EqualTo(l[i].key) {
			m.m[h] = append(l[:i], l[i+1:]...)
			m.len -= 1
			return l[i].value
		}
	}

	return nil
}

func (m *HashMap) List() []interface{} {
	r := make([]interface{}, 0, m.len)
	for _, l := range m.m {
		for _, e := range l {
			r = append(r, e.value)
		}
	}
	return r
}

func (m *HashMap) Len() int {
	return m.len
}
