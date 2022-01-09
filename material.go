package coral

import "sync"

type M struct {
	item interface{}

	// wg is used for process control
	wg *sync.WaitGroup

	// status represents M's current status
	status int
}

func newM(item interface{}) *M {
	return &M{item: item}
}

func (m *M) setWG(wg *sync.WaitGroup) *M {
	m.wg = wg
	return m
}

func (m *M) done() {
	if m.wg != nil {
		m.wg.Done()
	}
}

func (m *M) Item() interface{} {
	return m.item
}
