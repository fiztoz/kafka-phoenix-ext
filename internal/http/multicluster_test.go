package http

import (
	"context"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/fiztoz/kafka-phoenix-ext/internal/kafka"
	"github.com/fiztoz/kafka-phoenix-ext/internal/poller"
	"github.com/fiztoz/kafka-phoenix-ext/internal/store"
)

// multiSource is a ClusterSource over several clusters with independent
// snapshots and an independent StaleThreshold.
type multiSource struct {
	mu    sync.RWMutex
	order []string
	snaps map[string]poller.Snapshot
	stale time.Duration
}

func newMultiSource(order []string, snaps map[string]poller.Snapshot) *multiSource {
	if snaps == nil {
		snaps = map[string]poller.Snapshot{}
	}
	return &multiSource{order: order, snaps: snaps, stale: 30 * time.Minute}
}

func (m *multiSource) Clusters() []string { return append([]string(nil), m.order...) }

func (m *multiSource) Snapshot(cluster string) (poller.Snapshot, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	s, ok := m.snaps[cluster]
	return s, ok
}

func (m *multiSource) StaleThreshold() time.Duration { return m.stale }

func (m *multiSource) set(cluster string, s poller.Snapshot) {
	m.mu.Lock()
	m.snaps[cluster] = s
	m.mu.Unlock()
}

func (m *multiSource) RefreshThresholds(context.Context, string) {}

// multiStore keeps one threshold map per cluster so a test can prove a write
// landed in the right cluster.
type multiStore struct {
	mu   sync.Mutex
	th   map[string]map[string]store.ThresholdRow
	last string
}

func newMultiStore() *multiStore {
	return &multiStore{th: map[string]map[string]store.ThresholdRow{}}
}

func (m *multiStore) Scoped(cluster string) store.Scoped {
	return &multiScope{root: m, cluster: cluster}
}
func (m *multiStore) Migrate(context.Context) error { return nil }
func (m *multiStore) Close() error                  { return nil }

func (m *multiStore) rows(cluster string) map[string]store.ThresholdRow {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.th[cluster] == nil {
		m.th[cluster] = map[string]store.ThresholdRow{}
	}
	return m.th[cluster]
}

func (m *multiStore) clusterRows(cluster string) map[string]store.ThresholdRow {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := map[string]store.ThresholdRow{}
	for k, v := range m.th[cluster] {
		out[k] = v
	}
	return out
}

type multiScope struct {
	root    *multiStore
	cluster string
}

func (s *multiScope) Cluster() string { return s.cluster }

func (s *multiScope) UpsertStates(context.Context, []store.StateRow) error { return nil }
func (s *multiScope) States(context.Context) ([]store.StateRow, error)     { return nil, nil }

func (s *multiScope) SetThreshold(_ context.Context, topic string, limit int64, warn, growth *int64) error {
	rows := s.root.rows(s.cluster)
	s.root.mu.Lock()
	defer s.root.mu.Unlock()
	rows[topic] = store.ThresholdRow{
		Cluster: s.cluster, Topic: topic, ThresholdBytes: limit,
		WarnBytes: warn, GrowthPerHour: growth, UpdatedAt: time.Now(),
	}
	s.root.last = s.cluster
	return nil
}

func (s *multiScope) DeleteThreshold(_ context.Context, topic string) error {
	rows := s.root.rows(s.cluster)
	s.root.mu.Lock()
	defer s.root.mu.Unlock()
	delete(rows, topic)
	s.root.last = s.cluster
	return nil
}

func (s *multiScope) Thresholds(context.Context) (map[string]store.ThresholdRow, error) {
	return s.root.clusterRows(s.cluster), nil
}

// clusterSnapshot builds an observation with one topic named after the
// cluster, so a test can tell the two dashboards apart by body content.
func clusterSnapshot(cluster, topic string, bytes int64, pollOK bool, lastErr string) poller.Snapshot {
	now := time.Date(2026, 11, 1, 10, 0, 0, 0, time.UTC)
	limit := int64(1 << 20)
	snap := poller.Snapshot{
		Cluster:      cluster,
		ClusterID:    "kafka-" + cluster,
		ControllerID: 1,
		PolledAt:     now,
		PollOK:       pollOK,
		LastError:    lastErr,
		TotalBytes:   bytes,
		Brokers:      []kafka.BrokerUse{{ID: 1, Host: "broker-" + cluster, Bytes: bytes, Partitions: 1}},
		Topics: []poller.TopicView{{
			Name: topic, Partitions: 1, Replicas: 1,
			StorageBytes: bytes, LeaderBytes: bytes,
			ThresholdBytes: &limit,
		}},
	}
	return snap
}

func newMultiServer(t *testing.T, legacy string, snaps map[string]poller.Snapshot, order []string) (*Server, *multiSource, *multiStore) {
	t.Helper()
	src := newMultiSource(order, snaps)
	st := newMultiStore()
	srv, err := New(Deps{
		BasePath:      "/kafka",
		Clusters:      src,
		Store:         st,
		Log:           slog.New(slog.DiscardHandler),
		LegacyCluster: legacy,
	})
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	return srv, src, st
}

func twoClusters() (map[string]poller.Snapshot, []string) {
	return map[string]poller.Snapshot{
		"harbor": clusterSnapshot("harbor", "harbor-orders", 1024, true, ""),
		"summit": clusterSnapshot("summit", "summit-orders", 2048, true, ""),
	}, []string{"harbor", "summit"}
}

func TestMultiClusterDashboardsAreSeparate(t *testing.T) {
	snaps, order := twoClusters()
	srv, _, _ := newMultiServer(t, "harbor", snaps, order)

	harbor := do(t, srv.Handler(), http.MethodGet, "/kafka/c/harbor/", nil, nil)
	if harbor.Code != http.StatusOK {
		t.Fatalf("harbor dashboard = %d", harbor.Code)
	}
	if !strings.Contains(harbor.Body.String(), "harbor-orders") || strings.Contains(harbor.Body.String(), "summit-orders") {
		t.Fatalf("harbor dashboard leaked the other cluster:\n%s", harbor.Body.String())
	}

	summit := do(t, srv.Handler(), http.MethodGet, "/kafka/c/summit/", nil, nil)
	if summit.Code != http.StatusOK {
		t.Fatalf("summit dashboard = %d", summit.Code)
	}
	if !strings.Contains(summit.Body.String(), "summit-orders") || strings.Contains(summit.Body.String(), "harbor-orders") {
		t.Fatal("summit dashboard leaked the other cluster")
	}

	// Every link inside a cluster page stays inside that cluster.
	if !strings.Contains(summit.Body.String(), `href="/kafka/c/summit/topic/summit-orders"`) {
		t.Fatal("summit topic link is not cluster-scoped")
	}
	if !strings.Contains(summit.Body.String(), `href="/kafka/c/summit/wallboard"`) {
		t.Fatal("summit wallboard link is not cluster-scoped")
	}
	if strings.Contains(summit.Body.String(), `href="/kafka/topic/`) {
		t.Fatal("summit page still emits unprefixed legacy topic links")
	}
}

func TestMultiClusterOverviewIsTheLandingPage(t *testing.T) {
	snaps, order := twoClusters()
	srv, _, _ := newMultiServer(t, "harbor", snaps, order)

	w := do(t, srv.Handler(), http.MethodGet, "/kafka/", nil, nil)
	if w.Code != http.StatusOK {
		t.Fatalf("landing = %d", w.Code)
	}
	body := w.Body.String()
	for _, want := range []string{
		`href="/kafka/c/harbor"`, `href="/kafka/c/summit"`,
		"2 clusters", "harbor", "summit",
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("overview missing %q", want)
		}
	}
	// The overview must not pretend to be one cluster's dashboard.
	if strings.Contains(body, `id="filter"`) {
		t.Fatal("landing rendered the topic dashboard instead of the overview")
	}
}

// A single-cluster install must keep the original flat URLs.
func TestSingleClusterKeepsFlatURLs(t *testing.T) {
	snaps := map[string]poller.Snapshot{"default": clusterSnapshot("default", "orders", 1024, true, "")}
	srv, _, _ := newMultiServer(t, "default", snaps, []string{"default"})

	w := do(t, srv.Handler(), http.MethodGet, "/kafka/", nil, nil)
	body := w.Body.String()
	for _, want := range []string{`href="/kafka/topic/orders"`, `href="/kafka/wallboard"`, `id="filter"`} {
		if !strings.Contains(body, want) {
			t.Fatalf("single-cluster dashboard missing %q", want)
		}
	}
	if strings.Contains(body, "/kafka/c/") {
		t.Fatal("single-cluster install should not link to /c/ URLs")
	}
	// The clusternav is hidden when only one cluster is polled.
	if strings.Contains(body, `class="clusters"`) {
		t.Fatal("cluster switcher should be hidden with one cluster")
	}
}

func TestLegacyRoutesBindTheLegacyCluster(t *testing.T) {
	snaps, order := twoClusters()
	srv, _, _ := newMultiServer(t, "summit", snaps, order)

	// Unprefixed topic access resolves against the legacy cluster only.
	if w := do(t, srv.Handler(), http.MethodGet, "/kafka/topic/summit-orders", nil, nil); w.Code != http.StatusOK {
		t.Fatalf("legacy topic on summit = %d", w.Code)
	}
	if w := do(t, srv.Handler(), http.MethodGet, "/kafka/topic/harbor-orders", nil, nil); w.Code != http.StatusNotFound {
		t.Fatalf("legacy route must not reach harbor, got %d", w.Code)
	}
	if w := do(t, srv.Handler(), http.MethodGet, "/kafka/api/topics", nil, nil); w.Code != http.StatusOK ||
		!strings.Contains(w.Body.String(), `"cluster":"summit"`) {
		t.Fatalf("legacy api = %d %s", w.Code, w.Body.String())
	}
}

func TestPerClusterHealthEndpoints(t *testing.T) {
	snaps, order := twoClusters()
	snaps["summit"] = clusterSnapshot("summit", "summit-orders", 2048, false, "kafka: describe log dirs: CLUSTER_AUTHORIZATION_FAILED")
	srv, _, _ := newMultiServer(t, "harbor", snaps, order)

	if w := do(t, srv.Handler(), http.MethodGet, "/kafka/health/c/harbor/ready", nil, nil); w.Code != http.StatusOK {
		t.Fatalf("harbor ready = %d %s", w.Code, w.Body.String())
	}
	w := do(t, srv.Handler(), http.MethodGet, "/kafka/health/c/summit/ready", nil, nil)
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("summit ready = %d", w.Code)
	}
	// An ACL failure must never be reported as "unreachable".
	if !strings.Contains(w.Body.String(), "missing ACLs") {
		t.Fatalf("summit ready body = %q", w.Body.String())
	}
	// Legacy ready follows the legacy cluster (harbor), not the broken one.
	if w := do(t, srv.Handler(), http.MethodGet, "/kafka/health/ready", nil, nil); w.Code != http.StatusOK {
		t.Fatalf("legacy ready = %d, want harbor's ok", w.Code)
	}

	// /health/clusters aggregates: one broken cluster fails the whole monitor.
	w = do(t, srv.Handler(), http.MethodGet, "/kafka/health/clusters", nil, nil)
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("clusters health = %d", w.Code)
	}
	if !strings.Contains(w.Body.String(), "summit") || strings.Contains(w.Body.String(), "harbor (") {
		t.Fatalf("clusters health should name only summit: %q", w.Body.String())
	}
}

func TestHealthClustersOKWhenAllPoll(t *testing.T) {
	snaps, order := twoClusters()
	srv, _, _ := newMultiServer(t, "harbor", snaps, order)
	w := do(t, srv.Handler(), http.MethodGet, "/kafka/health/clusters", nil, nil)
	if w.Code != http.StatusOK || !strings.Contains(w.Body.String(), "ok") {
		t.Fatalf("clusters health = %d %q", w.Code, w.Body.String())
	}
}

// Threshold writes must land on the cluster named in the route.
func TestThresholdFormWritesToTheRoutedCluster(t *testing.T) {
	snaps, order := twoClusters()
	srv, _, st := newMultiServer(t, "harbor", snaps, order)

	form := "topic=summit-orders&threshold_bytes=4096"
	w := do(t, srv.Handler(), http.MethodPost, "/kafka/c/summit/thresholds",
		map[string]string{"Content-Type": "application/x-www-form-urlencoded"}, strings.NewReader(form))
	if w.Code != http.StatusSeeOther {
		t.Fatalf("threshold post = %d %s", w.Code, w.Body.String())
	}
	if loc := w.Header().Get("Location"); loc != "/kafka/c/summit/" {
		t.Fatalf("redirect = %q, want the summit dashboard", loc)
	}
	if got := st.clusterRows("summit")["summit-orders"].ThresholdBytes; got != 4096 {
		t.Fatalf("summit threshold = %d", got)
	}
	if len(st.clusterRows("harbor")) != 0 {
		t.Fatal("threshold write leaked into harbor")
	}

	// The same topic name in the other cluster is a different row.
	form = "topic=harbor-orders&threshold_bytes=8192"
	w = do(t, srv.Handler(), http.MethodPost, "/kafka/c/harbor/thresholds",
		map[string]string{"Content-Type": "application/x-www-form-urlencoded"}, strings.NewReader(form))
	if w.Code != http.StatusSeeOther {
		t.Fatalf("harbor threshold post = %d", w.Code)
	}
	if got := st.clusterRows("harbor")["harbor-orders"].ThresholdBytes; got != 8192 {
		t.Fatalf("harbor threshold = %d", got)
	}
	if got := st.clusterRows("summit")["summit-orders"].ThresholdBytes; got != 4096 {
		t.Fatalf("summit threshold changed by harbor write: %d", got)
	}
}

// A topic observed on another cluster must not be settable here.
func TestThresholdRejectsTopicFromAnotherCluster(t *testing.T) {
	snaps, order := twoClusters()
	srv, _, st := newMultiServer(t, "harbor", snaps, order)

	w := do(t, srv.Handler(), http.MethodPost, "/kafka/c/harbor/api/thresholds",
		nil, strings.NewReader(`{"topic":"summit-orders","threshold_bytes":1024}`))
	if w.Code != http.StatusNotFound {
		t.Fatalf("cross-cluster threshold = %d, want 404", w.Code)
	}
	if len(st.clusterRows("harbor")) != 0 {
		t.Fatal("rejected threshold was still stored")
	}
}

func TestUnknownClusterPathIsNotFound(t *testing.T) {
	snaps, order := twoClusters()
	srv, _, _ := newMultiServer(t, "harbor", snaps, order)
	if w := do(t, srv.Handler(), http.MethodGet, "/kafka/c/staging/", nil, nil); w.Code != http.StatusNotFound {
		t.Fatalf("unknown cluster page = %d, want 404", w.Code)
	}
	if w := do(t, srv.Handler(), http.MethodGet, "/kafka/health/c/staging/ready", nil, nil); w.Code != http.StatusNotFound {
		t.Fatalf("unknown cluster health = %d, want 404", w.Code)
	}
}

func TestAPICarriesTheClusterName(t *testing.T) {
	snaps, order := twoClusters()
	srv, _, _ := newMultiServer(t, "harbor", snaps, order)

	w := do(t, srv.Handler(), http.MethodGet, "/kafka/c/summit/api/topics", nil, nil)
	if w.Code != http.StatusOK {
		t.Fatalf("summit api = %d", w.Code)
	}
	for _, want := range []string{`"cluster":"summit"`, `"cluster_id":"kafka-summit"`, `"name":"summit-orders"`} {
		if !strings.Contains(w.Body.String(), want) {
			t.Fatalf("summit api missing %q: %s", want, w.Body.String())
		}
	}

	w = do(t, srv.Handler(), http.MethodGet, "/kafka/c/harbor/api/brokers", nil, nil)
	if !strings.Contains(w.Body.String(), `"cluster":"harbor"`) || !strings.Contains(w.Body.String(), "broker-harbor") {
		t.Fatalf("harbor brokers api = %s", w.Body.String())
	}
}

// The cluster switcher links every polled cluster and marks the active one.
func TestClusterSwitcherMarksActive(t *testing.T) {
	snaps, order := twoClusters()
	srv, _, _ := newMultiServer(t, "harbor", snaps, order)

	body := do(t, srv.Handler(), http.MethodGet, "/kafka/c/summit/", nil, nil).Body.String()
	if !strings.Contains(body, `class="clusters"`) {
		t.Fatal("multi-cluster page is missing the switcher")
	}
	if !strings.Contains(body, `class="clink active" href="/kafka/c/summit" aria-current="page"`) {
		t.Fatal("active cluster is not marked in the switcher")
	}
	if !strings.Contains(body, `href="/kafka/c/harbor"`) {
		t.Fatal("switcher missing the other cluster")
	}
	if !strings.Contains(body, `href="/kafka/"`) {
		t.Fatal("switcher missing the all-clusters link")
	}
}

// Wallboard and broker drill-down must follow the same cluster scoping.
func TestWallboardAndBrokerAreClusterScoped(t *testing.T) {
	snaps, order := twoClusters()
	srv, _, _ := newMultiServer(t, "harbor", snaps, order)

	w := do(t, srv.Handler(), http.MethodGet, "/kafka/c/summit/wallboard", nil, nil)
	if w.Code != http.StatusOK || !strings.Contains(w.Body.String(), "summit-orders") {
		t.Fatalf("summit wallboard = %d", w.Code)
	}
	if strings.Contains(w.Body.String(), "harbor-orders") {
		t.Fatal("summit wallboard leaked harbor topics")
	}
	w = do(t, srv.Handler(), http.MethodGet, "/kafka/c/summit/broker/1", nil, nil)
	if w.Code != http.StatusOK || !strings.Contains(w.Body.String(), "broker-summit") {
		t.Fatalf("summit broker page = %d", w.Code)
	}
}

// One cluster failing must leave the other cluster's page healthy.
func TestFailedClusterDoesNotBreakTheOtherPage(t *testing.T) {
	snaps, order := twoClusters()
	snaps["harbor"] = clusterSnapshot("harbor", "harbor-orders", 1024, false, "deadline exceeded")
	srv, _, _ := newMultiServer(t, "harbor", snaps, order)

	w := do(t, srv.Handler(), http.MethodGet, "/kafka/c/summit/", nil, nil)
	body := w.Body.String()
	if w.Code != http.StatusOK || !strings.Contains(body, "summit-orders") {
		t.Fatalf("summit page = %d", w.Code)
	}
	if strings.Contains(body, "unreachable") || strings.Contains(body, "deadline exceeded") {
		t.Fatal("summit page shows harbor's failure banner")
	}
	// harbor's own page explains its failure.
	harborBody := do(t, srv.Handler(), http.MethodGet, "/kafka/c/harbor/", nil, nil).Body.String()
	if !strings.Contains(harborBody, "timed out") {
		t.Fatal("harbor page does not report its timeout")
	}
}
