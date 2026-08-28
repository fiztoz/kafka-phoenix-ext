package http

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/fiztoz/kafka-phoenix-ext/internal/kafka"
	"github.com/fiztoz/kafka-phoenix-ext/internal/poller"
	"github.com/fiztoz/kafka-phoenix-ext/internal/store"
)

type fakeSnapshots struct {
	mu    sync.RWMutex
	snap  poller.Snapshot
	stale time.Duration
}

func (f *fakeSnapshots) set(s poller.Snapshot) {
	f.mu.Lock()
	f.snap = s
	f.mu.Unlock()
}

func (f *fakeSnapshots) Snapshot() poller.Snapshot {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.snap
}

func (f *fakeSnapshots) StaleThreshold() time.Duration { return 30 * time.Minute }

type fakeStore struct {
	mu         sync.Mutex
	thresholds map[string]store.ThresholdRow
}

func newFakeStore() *fakeStore { return &fakeStore{thresholds: map[string]store.ThresholdRow{}} }

func (f *fakeStore) Migrate(context.Context) error { return nil }
func (f *fakeStore) Close() error                  { return nil }

func (f *fakeStore) UpsertStates(context.Context, []store.StateRow) error { return nil }
func (f *fakeStore) States(context.Context) ([]store.StateRow, error)     { return nil, nil }

func (f *fakeStore) SetThreshold(_ context.Context, topic string, limit int64, warn *int64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.thresholds[topic] = store.ThresholdRow{Topic: topic, ThresholdBytes: limit, WarnBytes: warn, UpdatedAt: time.Now()}
	return nil
}

func (f *fakeStore) DeleteThreshold(_ context.Context, topic string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.thresholds, topic)
	return nil
}

func (f *fakeStore) Thresholds(context.Context) (map[string]store.ThresholdRow, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make(map[string]store.ThresholdRow, len(f.thresholds))
	for k, v := range f.thresholds {
		out[k] = v
	}
	return out, nil
}

func newTestServer(t *testing.T, snap poller.Snapshot, uiToken string) (*Server, *fakeSnapshots, *fakeStore) {
	t.Helper()
	fs := &fakeSnapshots{snap: snap, stale: 30 * time.Minute}
	fs.set(snap)
	st := newFakeStore()
	srv, err := New(Deps{
		BasePath:  "/kafka",
		UIToken:   uiToken,
		Snapshots: fs,
		Store:     st,
		Log:       slog.New(slog.DiscardHandler),
	})
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	return srv, fs, st
}

func sampleSnapshot() poller.Snapshot {
	now := time.Date(2026, 8, 23, 12, 0, 0, 0, time.UTC)
	limit := int64(4096)
	warn := int64(2048)
	return poller.Snapshot{
		ClusterID:    "c1",
		ControllerID: 0,
		PolledAt:     now,
		PollOK:       true,
		TotalBytes:   3072,
		Brokers:      []kafka.BrokerUse{{ID: 0, Host: "kafka-0", Bytes: 1536, Partitions: 2}},
		Topics: []poller.TopicView{
			{
				Name: "orders", Partitions: 2, Replicas: 4,
				StorageBytes:   2048,
				LeaderBytes:    1024,
				GrowthPerHour:  128,
				ThresholdBytes: &limit,
				WarnBytes:      &warn,
				OverStreak:     2,
				ConfirmedOver:  true,
				Parts: []kafka.PartitionUse{
					{Partition: 0, Leader: 0, Replicas: []int32{0, 1}, ISR: []int32{0, 1}, SizeBytes: 512},
					{Partition: 1, Leader: 0, Replicas: []int32{0, 1}, ISR: []int32{0}, SizeBytes: 512, UnderRep: true},
				},
			},
			{Name: "events", Partitions: 1, Replicas: 2, StorageBytes: 1024, LeaderBytes: 512},
		},
	}
}

func do(t *testing.T, h http.Handler, method, path string, header map[string]string, body io.Reader) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(method, path, body)
	if body != nil && !strings.Contains(path, "/thresholds") {
		req.Header.Set("Content-Type", "application/json")
	}
	for k, v := range header {
		req.Header.Set(k, v)
	}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	return w
}

func TestHealthOpenWithoutToken(t *testing.T) {
	srv, _, _ := newTestServer(t, sampleSnapshot(), "secret")

	w := do(t, srv.Handler(), http.MethodGet, "/kafka/health/live", nil, nil)
	if w.Code != http.StatusOK || w.Body.String() != "ok" {
		t.Fatalf("live: %d %q", w.Code, w.Body.String())
	}

	w = do(t, srv.Handler(), http.MethodGet, "/kafka/health/ready", nil, nil)
	if w.Code != http.StatusOK {
		t.Fatalf("ready: %d", w.Code)
	}

	failing := sampleSnapshot()
	failing.PollOK = false
	failing.LastError = "describe: cluster unreachable"
	srv2, _, _ := newTestServer(t, failing, "secret")
	w = do(t, srv2.Handler(), http.MethodGet, "/kafka/health/ready", nil, nil)
	if w.Code != http.StatusServiceUnavailable || !strings.Contains(w.Body.String(), "cluster unreachable") {
		t.Fatalf("ready failing: %d %q", w.Code, w.Body.String())
	}
}

func TestThresholdsHealth(t *testing.T) {
	srv, _, _ := newTestServer(t, sampleSnapshot(), "")

	w := do(t, srv.Handler(), http.MethodGet, "/kafka/health/thresholds", nil, nil)
	if w.Code != http.StatusServiceUnavailable || !strings.Contains(w.Body.String(), "orders") {
		t.Fatalf("all: %d %q", w.Code, w.Body.String())
	}

	w = do(t, srv.Handler(), http.MethodGet, "/kafka/health/thresholds/orders", nil, nil)
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("orders: %d", w.Code)
	}

	w = do(t, srv.Handler(), http.MethodGet, "/kafka/health/thresholds/events", nil, nil)
	if w.Code != http.StatusOK {
		t.Fatalf("events (no threshold): %d %q", w.Code, w.Body.String())
	}

	w = do(t, srv.Handler(), http.MethodGet, "/kafka/health/thresholds/nope", nil, nil)
	if w.Code != http.StatusNotFound {
		t.Fatalf("unknown topic: %d", w.Code)
	}
}

func TestUIAuthTokenHandOff(t *testing.T) {
	srv, _, _ := newTestServer(t, sampleSnapshot(), "s3cret")

	// No credential → 401 on the dashboard.
	w := do(t, srv.Handler(), http.MethodGet, "/kafka/", nil, nil)
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("no cred: %d", w.Code)
	}

	// Phoenix /frame hands off ?ui_token= → 200 + session cookie.
	w = do(t, srv.Handler(), http.MethodGet, "/kafka/?ui_token=s3cret", nil, nil)
	if w.Code != http.StatusOK {
		t.Fatalf("hand-off: %d", w.Code)
	}
	var cookie *http.Cookie
	for _, c := range w.Result().Cookies() {
		if c.Name == "kafka_ui_session" {
			cookie = c
		}
	}
	if cookie == nil || !cookie.HttpOnly || cookie.Path != "/kafka" {
		t.Fatalf("session cookie missing or misconfigured: %+v", cookie)
	}

	// Follow-up navigation uses only the cookie, no token in the URL.
	req := httptest.NewRequest(http.MethodGet, "/kafka/", nil)
	req.AddCookie(cookie)
	w = httptest.NewRecorder()
	srv.Handler().ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("cookie nav: %d", w.Code)
	}

	// Wrong token → 401.
	w = do(t, srv.Handler(), http.MethodGet, "/kafka/?ui_token=wrong", nil, nil)
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("wrong token: %d", w.Code)
	}

	// Bearer header also works (API path).
	w = do(t, srv.Handler(), http.MethodGet, "/kafka/api/topics",
		map[string]string{"Authorization": "Bearer s3cret"}, nil)
	if w.Code != http.StatusOK {
		t.Fatalf("bearer API: %d", w.Code)
	}
}

func TestAPIThresholdsJSON(t *testing.T) {
	srv, _, st := newTestServer(t, sampleSnapshot(), "")

	// Valid threshold on a known topic.
	w := do(t, srv.Handler(), http.MethodPost, "/kafka/api/thresholds",
		nil, strings.NewReader(`{"topic":"events","threshold_bytes":1024,"warn_bytes":512}`))
	if w.Code != http.StatusOK {
		t.Fatalf("set: %d %s", w.Code, w.Body.String())
	}
	if _, ok := st.thresholds["events"]; !ok {
		t.Fatal("threshold not stored")
	}

	// Unknown topic → 404.
	w = do(t, srv.Handler(), http.MethodPost, "/kafka/api/thresholds",
		nil, strings.NewReader(`{"topic":"typo","threshold_bytes":1024}`))
	if w.Code != http.StatusNotFound {
		t.Fatalf("unknown topic: %d", w.Code)
	}

	// warn >= limit → 400.
	w = do(t, srv.Handler(), http.MethodPost, "/kafka/api/thresholds",
		nil, strings.NewReader(`{"topic":"events","threshold_bytes":1024,"warn_bytes":2048}`))
	if w.Code != http.StatusBadRequest {
		t.Fatalf("warn >= limit: %d", w.Code)
	}

	// Delete.
	w = do(t, srv.Handler(), http.MethodDelete, "/kafka/api/thresholds/events", nil, nil)
	if w.Code != http.StatusNoContent {
		t.Fatalf("delete: %d", w.Code)
	}
	if _, ok := st.thresholds["events"]; ok {
		t.Fatal("threshold not deleted")
	}

	// Invalid body → 400.
	w = do(t, srv.Handler(), http.MethodPost, "/kafka/api/thresholds", nil, strings.NewReader(`{`))
	if w.Code != http.StatusBadRequest {
		t.Fatalf("invalid JSON: %d", w.Code)
	}
}

func TestDashboardAndTopicPages(t *testing.T) {
	srv, _, _ := newTestServer(t, sampleSnapshot(), "")

	w := do(t, srv.Handler(), http.MethodGet, "/kafka/", nil, nil)
	if w.Code != http.StatusOK || !strings.Contains(w.Body.String(), "orders") {
		t.Fatalf("dashboard: %d", w.Code)
	}
	if !strings.Contains(w.Body.String(), "2.0 KiB") {
		t.Fatalf("humanized bytes missing: %s", w.Body.String())
	}

	w = do(t, srv.Handler(), http.MethodGet, "/kafka/topic/orders", nil, nil)
	if w.Code != http.StatusOK || !strings.Contains(w.Body.String(), "under-replicated") {
		t.Fatalf("topic page: %d", w.Code)
	}

	w = do(t, srv.Handler(), http.MethodGet, "/kafka/wallboard", nil, nil)
	if w.Code != http.StatusOK || !strings.Contains(w.Body.String(), "orders") {
		t.Fatalf("wallboard: %d", w.Code)
	}

	w = do(t, srv.Handler(), http.MethodGet, "/kafka/topic/missing", nil, nil)
	if w.Code != http.StatusNotFound {
		t.Fatalf("unknown topic page: %d", w.Code)
	}
}

func TestThresholdFormRoundTrip(t *testing.T) {
	srv, _, st := newTestServer(t, sampleSnapshot(), "")

	form := "topic=events&threshold_bytes=2048&warn_bytes=1024"
	w := do(t, srv.Handler(), http.MethodPost, "/kafka/thresholds",
		map[string]string{"Content-Type": "application/x-www-form-urlencoded"}, strings.NewReader(form))
	if w.Code != http.StatusSeeOther {
		t.Fatalf("form set: %d %s", w.Code, w.Body.String())
	}
	if _, ok := st.thresholds["events"]; !ok {
		t.Fatal("form threshold not stored")
	}

	w = do(t, srv.Handler(), http.MethodPost, "/kafka/thresholds/delete",
		map[string]string{"Content-Type": "application/x-www-form-urlencoded"},
		strings.NewReader("topic=events"))
	if w.Code != http.StatusSeeOther {
		t.Fatalf("form delete: %d", w.Code)
	}
	if _, ok := st.thresholds["events"]; ok {
		t.Fatal("form threshold not deleted")
	}
}

func TestApiTopicsJSONShape(t *testing.T) {
	srv, _, _ := newTestServer(t, sampleSnapshot(), "")

	w := do(t, srv.Handler(), http.MethodGet, "/kafka/api/topics", nil, nil)
	if w.Code != http.StatusOK {
		t.Fatalf("api topics: %d", w.Code)
	}
	body := w.Body.String()
	for _, want := range []string{
		`"cluster_id":"c1"`,
		`"storage_bytes":2048`,
		`"growth_per_hour":128`,
		`"confirmed_over":true`,
		`"under_replicated_partitions"`,
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("api topics missing %s: %s", want, body)
		}
	}

	w = do(t, srv.Handler(), http.MethodGet, "/kafka/api/topics/orders", nil, nil)
	if w.Code != http.StatusOK || !strings.Contains(w.Body.String(), `"partition":1`) {
		t.Fatalf("api topic detail: %d %s", w.Code, w.Body.String())
	}

	w = do(t, srv.Handler(), http.MethodGet, "/kafka/api/topics/missing", nil, nil)
	if w.Code != http.StatusNotFound {
		t.Fatalf("api unknown topic: %d", w.Code)
	}
}

func TestRootPathConvenience(t *testing.T) {
	// When BASE_PATH != "/", the mux also registers routes at the root so
	// local go run works without an Ingress prefix.
	srv, _, _ := newTestServer(t, sampleSnapshot(), "")
	w := do(t, srv.Handler(), http.MethodGet, "/health/live", nil, nil)
	if w.Code != http.StatusOK {
		t.Fatalf("root health: %d", w.Code)
	}
}
