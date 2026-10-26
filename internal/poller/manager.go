package poller

import (
	"context"
	"sync"
	"time"
)

// Manager owns one Poller per configured Kafka cluster and is the single
// source the HTTP layer reads. Clusters keeps declaration order so the UI and
// the health endpoints stay stable across restarts.
type Manager struct {
	order    []string
	byID     map[string]*Poller
	interval time.Duration
}

// NewManager builds a Manager over already-constructed pollers. Every poller
// must share the same interval: StaleThreshold is derived from it.
func NewManager(interval time.Duration, pollers ...*Poller) *Manager {
	m := &Manager{
		order:    make([]string, 0, len(pollers)),
		byID:     make(map[string]*Poller, len(pollers)),
		interval: interval,
	}
	for _, p := range pollers {
		m.order = append(m.order, p.cluster)
		m.byID[p.cluster] = p
	}
	return m
}

// Clusters returns the configured cluster ids in declaration order.
func (m *Manager) Clusters() []string {
	return append([]string(nil), m.order...)
}

// Len reports how many clusters are polled.
func (m *Manager) Len() int { return len(m.order) }

// Has reports whether a cluster id is configured. Callers use it to reject a
// path segment before touching a snapshot.
func (m *Manager) Has(cluster string) bool {
	_, ok := m.byID[cluster]
	return ok
}

// Snapshot returns one cluster's last observation. The bool is false for an
// unknown cluster id, letting the HTTP layer answer 404 without leaking which
// clusters exist.
func (m *Manager) Snapshot(cluster string) (Snapshot, bool) {
	p, ok := m.byID[cluster]
	if !ok {
		return Snapshot{}, false
	}
	return p.Snapshot(), true
}

// Snapshots returns every cluster's observation in declaration order.
func (m *Manager) Snapshots() []Snapshot {
	out := make([]Snapshot, 0, len(m.order))
	for _, id := range m.order {
		if p, ok := m.byID[id]; ok {
			out = append(out, p.Snapshot())
		}
	}
	return out
}

// StaleThreshold is how old a sample must be before the UI flags it stale.
// Every poller shares the configured interval, so one value covers them all.
func (m *Manager) StaleThreshold() time.Duration { return m.interval * 3 }

// RefreshThresholds reloads one cluster's thresholds after a UI mutation.
func (m *Manager) RefreshThresholds(ctx context.Context, cluster string) {
	if p, ok := m.byID[cluster]; ok {
		p.RefreshThresholds(ctx)
	}
}

// Run polls every cluster concurrently and returns once all pollers have
// stopped. A failure in one cluster's tick never stops the others: each
// Poller keeps its own last good sample and error state.
func (m *Manager) Run(ctx context.Context) {
	var wg sync.WaitGroup
	for _, id := range m.order {
		p, ok := m.byID[id]
		if !ok {
			continue
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			p.Run(ctx)
		}()
	}
	wg.Wait()
}

// PollOnce runs one observation for every cluster. Intended for tests and
// one-shot tooling; Run is the production loop.
func (m *Manager) PollOnce(ctx context.Context) {
	for _, id := range m.order {
		if p, ok := m.byID[id]; ok {
			p.PollOnce(ctx)
		}
	}
}
