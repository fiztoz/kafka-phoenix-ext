// Package http serves the kafka-phoenix-ext dashboard, wallboard, JSON API
// and health endpoints. It renders server-side Go templates — no JS framework.
package http

import (
	"context"
	"crypto/subtle"
	"embed"
	"encoding/json"
	"fmt"
	"html/template"
	"log/slog"
	"math"
	"net/http"
	"strings"
	"time"

	"github.com/fiztoz/kafka-phoenix-ext/internal/poller"
	"github.com/fiztoz/kafka-phoenix-ext/internal/store"
)

//go:embed views/styles.html views/dashboard.html views/topic.html views/wallboard.html views/broker.html views/icon.svg
var assets embed.FS

// SnapshotSource is what the HTTP layer reads from the poller.
type SnapshotSource interface {
	Snapshot() poller.Snapshot
	StaleThreshold() time.Duration
}

// thresholdRefresher is optionally implemented by SnapshotSource (the poller
// does) so threshold mutations become visible without waiting for the next
// poll.
type thresholdRefresher interface {
	RefreshThresholds(ctx context.Context)
}

func (s *Server) refreshThresholds(r *http.Request) {
	if rf, ok := s.deps.Snapshots.(thresholdRefresher); ok {
		rf.RefreshThresholds(r.Context())
	}
}

// Deps wires the server.
type Deps struct {
	BasePath  string // e.g. "/kafka"; "/" for local dev
	UIToken   string // empty = UI open (health is always open)
	Snapshots SnapshotSource
	Store     store.Store
	Log       *slog.Logger
}

// Server is the kafka-phoenix-ext HTTP server.
type Server struct {
	deps Deps
	tmpl *template.Template
	icon []byte
}

// New parses templates and builds the server.
func New(deps Deps) (*Server, error) {
	funcs := template.FuncMap{
		"bytes":  HumanBytes,
		"pct":    pct,
		"ts":     ts,
		"age":    age,
		"growth": Growth,
		"join":   joinInts,
		"rf":     replicationFactor,
		"num":    func(p *int64) string { return formatNum(p) },
		"numv":   func(p *int64) int64 { return numValue(p) },
		"pskew":  partSkew,
		"stale":  func(f bool) string { return map[bool]string{true: "stale", false: ""}[f] },
		"path":   JoinPath,
	}
	tmpl, err := template.New("").Funcs(funcs).ParseFS(assets,
		"views/styles.html", "views/dashboard.html", "views/topic.html",
		"views/wallboard.html", "views/broker.html")
	if err != nil {
		return nil, fmt.Errorf("http: parse templates: %w", err)
	}
	icon, err := assets.ReadFile("views/icon.svg")
	if err != nil {
		return nil, fmt.Errorf("http: read icon: %w", err)
	}
	return &Server{deps: deps, tmpl: tmpl, icon: icon}, nil
}

// Handler returns the mux with routes registered under BASE_PATH and, when
// BASE_PATH != "/", also at the root (local `go run` convenience and so
// probes work with or without the Ingress prefix).
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	s.register(mux, s.deps.BasePath)
	if s.deps.BasePath != "/" {
		s.register(mux, "/")
	}
	return mux
}

func (s *Server) register(mux *http.ServeMux, prefix string) {
	p := strings.TrimRight(prefix, "/")

	// Open routes.
	mux.HandleFunc("GET "+p+"/icon.svg", s.securityHeaders(s.handleIcon))
	mux.HandleFunc("GET "+p+"/health/live", s.securityHeaders(s.handleLive))
	mux.HandleFunc("GET "+p+"/health/ready", s.securityHeaders(s.handleReady))
	mux.HandleFunc("GET "+p+"/health/thresholds", s.securityHeaders(s.handleThresholdsAll))
	mux.HandleFunc("GET "+p+"/health/thresholds/{topic}", s.securityHeaders(s.handleThresholdsTopic))
	mux.HandleFunc("GET "+p+"/health/replicas", s.securityHeaders(s.handleHealthReplicas))
	mux.HandleFunc("GET "+p+"/health/growth", s.securityHeaders(s.handleHealthGrowth))
	mux.HandleFunc("GET "+p+"/health/brokers", s.securityHeaders(s.handleHealthBrokers))

	// UI-token-guarded routes (open when UI_TOKEN is empty).
	mux.HandleFunc("GET "+p+"/", s.securityHeaders(s.uiAuth(s.handleDashboard)))
	mux.HandleFunc("GET "+p+"/wallboard", s.securityHeaders(s.uiAuth(s.handleWallboard)))
	mux.HandleFunc("GET "+p+"/topic/{name}", s.securityHeaders(s.uiAuth(s.handleTopicPage)))
	mux.HandleFunc("GET "+p+"/broker/{id}", s.securityHeaders(s.uiAuth(s.handleBrokerPage)))
	mux.HandleFunc("POST "+p+"/thresholds", s.securityHeaders(s.uiAuth(s.handleThresholdForm)))
	mux.HandleFunc("POST "+p+"/thresholds/delete", s.securityHeaders(s.uiAuth(s.handleThresholdDeleteForm)))
	mux.HandleFunc("GET "+p+"/api/topics", s.securityHeaders(s.uiAuth(s.handleAPITopics)))
	mux.HandleFunc("GET "+p+"/api/topics/{topic}", s.securityHeaders(s.uiAuth(s.handleAPITopic)))
	mux.HandleFunc("GET "+p+"/api/brokers", s.securityHeaders(s.uiAuth(s.handleAPIBrokers)))
	mux.HandleFunc("POST "+p+"/api/thresholds", s.securityHeaders(s.uiAuth(s.handleAPISetThreshold)))
	mux.HandleFunc("DELETE "+p+"/api/thresholds/{topic}", s.securityHeaders(s.uiAuth(s.handleAPIDeleteThreshold)))
}

// securityHeaders sets the locked header set on every response.
// frame-ancestors 'self' lets a same-host Phoenix admin iframe work and
// blocks random sites. Never '*' in v1.
func (s *Server) securityHeaders(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		h := w.Header()
		h.Set("Content-Security-Policy", "frame-ancestors 'self'")
		h.Set("X-Content-Type-Options", "nosniff")
		h.Set("Referrer-Policy", "no-referrer")
		h.Set("Cache-Control", "no-store")
		next(w, r)
	}
}

// uiAuth guards dashboard/API/form routes when UI_TOKEN is configured.
// Health endpoints are deliberately NOT behind this guard: Phoenix HTTP
// monitors hit them without a Bearer token.
//
// Credential hand-off: a valid Authorization: Bearer or ui_token parameter
// (the form Phoenix's gated /frame redirect uses) is exchanged for a session
// cookie, because the extension's own links and form posts cannot carry the
// token forward. The iframe embedding Phoenix is same-host, so the cookie is
// first-party; SameSite=Lax keeps cross-site top-level POSTs from replaying
// it.
func (s *Server) uiAuth(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		want := s.deps.UIToken
		if want == "" {
			next(w, r)
			return
		}
		// Session cookie from an earlier hand-off.
		if c, err := r.Cookie(uiCookieName); err == nil &&
			subtle.ConstantTimeCompare([]byte(c.Value), []byte(want)) == 1 {
			next(w, r)
			return
		}
		got := bearerToken(r)
		if got == "" {
			if err := r.ParseForm(); err == nil {
				got = r.Form.Get("ui_token")
			}
		}
		if got == "" || subtle.ConstantTimeCompare([]byte(got), []byte(want)) != 1 {
			if strings.Contains(r.URL.Path, "/api/") ||
				strings.Contains(r.Header.Get("Accept"), "application/json") {
				writeJSONErr(w, http.StatusUnauthorized, "unauthorized")
				return
			}
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		// Valid credential: swap it for the session cookie so follow-up
		// navigations inside the extension stay authenticated without the
		// token reappearing in URLs.
		http.SetCookie(w, &http.Cookie{
			Name:     uiCookieName,
			Value:    want,
			Path:     uiCookiePath(s.deps.BasePath),
			HttpOnly: true,
			SameSite: http.SameSiteLaxMode,
			MaxAge:   12 * 60 * 60,
		})
		next(w, r)
	}
}

// uiCookieName is the session cookie set after a successful UI_TOKEN hand-off.
const uiCookieName = "kafka_ui_session"

// uiCookiePath scopes the session cookie to the extension's Ingress prefix so
// it is never sent to Phoenix or sibling extensions on the same host.
func uiCookiePath(basePath string) string {
	p := strings.TrimRight(basePath, "/")
	if p == "" {
		return "/"
	}
	return p
}

func bearerToken(r *http.Request) string {
	h := r.Header.Get("Authorization")
	const p = "Bearer "
	if len(h) > len(p) && strings.EqualFold(h[:len(p)], p) {
		return strings.TrimSpace(h[len(p):])
	}
	return ""
}

// --- health ---

func (s *Server) handleLive(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	_, _ = w.Write([]byte("ok"))
}

func (s *Server) handleReady(w http.ResponseWriter, _ *http.Request) {
	snap := s.deps.Snapshots.Snapshot()
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	if snap.PollOK {
		_, _ = w.Write([]byte("ok"))
		return
	}
	w.WriteHeader(http.StatusServiceUnavailable)
	if snap.LastError == "" {
		_, _ = w.Write([]byte("no successful poll yet"))
		return
	}
	// Classify so an ACL problem is never reported as "kafka unreachable":
	// the on-call response to each is different.
	if classifyErr(snap.LastError) == errAuth {
		_, _ = w.Write([]byte("authenticated but missing ACLs: " + snap.LastError))
		return
	}
	_, _ = w.Write([]byte(snap.LastError))
}

// handleHealthReplicas is 503 while any topic stays under-replicated or
// offline across confirmSamples polls. Metadata alone feeds this, so it
// needs no extra ACLs beyond what the poller already uses.
func (s *Server) handleHealthReplicas(w http.ResponseWriter, _ *http.Request) {
	snap := s.deps.Snapshots.Snapshot()
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	for _, t := range snap.Topics {
		if t.ReplicaConfirmed {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = fmt.Fprintf(w, "topic %s has under-replicated or offline partitions for %d consecutive polls",
				t.Name, t.RepStreak)
			return
		}
	}
	if snap.MetadataUnavailable || replicaObservationUnavailable(snap) {
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte("replica metadata unavailable"))
		return
	}
	_, _ = w.Write([]byte("ok"))
}

func replicaObservationUnavailable(snap poller.Snapshot) bool {
	for _, t := range snap.Topics {
		if t.ReplicaUnavailable {
			return true
		}
	}
	return false
}

// handleHealthGrowth is 503 while any topic's rolling growth rate stays at
// or above its operator-set growth limit across confirmSamples polls.
func (s *Server) handleHealthGrowth(w http.ResponseWriter, _ *http.Request) {
	snap := s.deps.Snapshots.Snapshot()
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	for _, t := range snap.Topics {
		if t.GrowthConfirmed {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = fmt.Fprintf(w, "topic %s growing %s/hr over growth limit %s/hr for %d consecutive polls",
				t.Name, Growth(t.GrowthPerHour), HumanBytes(numValue(t.GrowthThreshold)), t.GrowthStreak)
			return
		}
	}
	_, _ = w.Write([]byte("ok"))
}

// handleHealthBrokers is 503 while the hottest broker stays over the skew
// policy (share of cluster bytes or absolute cap) across confirmSamples
// polls.
func (s *Server) handleHealthBrokers(w http.ResponseWriter, _ *http.Request) {
	snap := s.deps.Snapshots.Snapshot()
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	if snap.SkewConfirmed {
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = fmt.Fprintf(w, "broker %d holds %.1f%% of cluster storage for %d consecutive polls",
			snap.SkewBrokerID, snap.SkewSharePct, snap.SkewStreak)
		return
	}
	_, _ = w.Write([]byte("ok"))
}

func (s *Server) handleThresholdsAll(w http.ResponseWriter, _ *http.Request) {
	snap := s.deps.Snapshots.Snapshot()
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	for _, t := range snap.Topics {
		if t.ConfirmedOver {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = fmt.Fprintf(w, "topic %s over threshold (%d consecutive samples)", t.Name, t.OverStreak)
			return
		}
	}
	_, _ = w.Write([]byte("ok"))
}

func (s *Server) handleThresholdsTopic(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("topic")
	snap := s.deps.Snapshots.Snapshot()
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	for _, t := range snap.Topics {
		if t.Name == name {
			if t.ThresholdBytes == nil {
				_, _ = w.Write([]byte("ok")) // no threshold set for this topic
				return
			}
			if t.ConfirmedOver {
				w.WriteHeader(http.StatusServiceUnavailable)
				_, _ = fmt.Fprintf(w, "topic %s over threshold", name)
				return
			}
			_, _ = w.Write([]byte("ok"))
			return
		}
	}
	// Unknown topic: 404 without leaking cluster topology.
	http.Error(w, "unknown topic", http.StatusNotFound)
}

// --- icon ---

func (s *Server) handleIcon(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "image/svg+xml")
	_, _ = w.Write(s.icon)
}

// --- JSON API ---

type apiTopic struct {
	Name           string `json:"name"`
	Partitions     int    `json:"partitions"`
	Replicas       int    `json:"replicas"`
	StorageBytes   int64  `json:"storage_bytes"`
	LeaderBytes    int64  `json:"leader_bytes"`
	GrowthPerHour  int64  `json:"growth_per_hour"`
	PrevBytes      int64  `json:"prev_bytes"`
	UnderRep       int    `json:"under_replicated_partitions"`
	Offline        int    `json:"offline_partitions"`
	Stale          bool   `json:"stale"`
	ThresholdBytes *int64 `json:"threshold_bytes"`
	WarnBytes      *int64 `json:"warn_bytes"`
	OverStreak     int    `json:"over_streak"`
	ConfirmedOver  bool   `json:"confirmed_over"`

	GrowthThreshold  *int64  `json:"growth_threshold_bytes_per_hour"`
	GrowthStreak     int     `json:"growth_over_streak"`
	GrowthConfirmed  bool    `json:"growth_confirmed"`
	HoursToLimit     *int64  `json:"hours_to_limit"`
	PartitionSkew    float64 `json:"partition_skew_max_over_avg"`
	RepStreak        int     `json:"replica_issue_streak"`
	ReplicaConfirmed bool    `json:"replica_issue_confirmed"`
}

type apiPartition struct {
	Partition int32   `json:"partition"`
	Leader    int32   `json:"leader"`
	Replicas  []int32 `json:"replicas"`
	ISR       []int32 `json:"isr"`
	SizeBytes int64   `json:"size_bytes"`
	OffsetLag int64   `json:"offset_lag"`
	UnderRep  bool    `json:"under_replicated"`
	Offline   bool    `json:"offline"`
}

type apiResponse struct {
	ClusterID           string      `json:"cluster_id"`
	ControllerID        int32       `json:"controller_id"`
	PolledAt            time.Time   `json:"polled_at"`
	PollOK              bool        `json:"poll_ok"`
	LastError           string      `json:"last_error"`
	StorageError        string      `json:"storage_error"`
	SizeIncomplete      bool        `json:"size_incomplete"`
	MetadataUnavailable bool        `json:"metadata_unavailable"`
	TotalBytes          int64       `json:"total_bytes"`
	StaleAfterSeconds   int64       `json:"stale_after_seconds"`
	Brokers             []apiBroker `json:"brokers"`
	BrokerErrors        []string    `json:"broker_errors"`
	Topics              []apiTopic  `json:"topics"`
}

type apiBroker struct {
	ID         int32  `json:"id"`
	Host       string `json:"host"`
	Bytes      int64  `json:"bytes"`
	Partitions int    `json:"partitions"`
}

// apiBrokerShare extends apiBroker with its share of broker-reported bytes.
type apiBrokerShare struct {
	apiBroker
	SharePct float64 `json:"share_pct"`
}

type apiBrokersResponse struct {
	ClusterID        string           `json:"cluster_id"`
	PolledAt         time.Time        `json:"polled_at"`
	PollOK           bool             `json:"poll_ok"`
	LastError        string           `json:"last_error"`
	TotalBrokerBytes int64            `json:"total_broker_bytes"`
	Brokers          []apiBrokerShare `json:"brokers"`
	SkewConfirmed    bool             `json:"skew_confirmed"`
	SkewStreak       int              `json:"skew_streak"`
	SkewBrokerID     int32            `json:"skew_broker_id"`
	SkewSharePct     float64          `json:"skew_share_pct"`
}

func (s *Server) handleAPITopics(w http.ResponseWriter, _ *http.Request) {
	snap := s.deps.Snapshots.Snapshot()
	writeJSON(w, http.StatusOK, toAPIResponse(s, snap, nil))
}

func (s *Server) handleAPITopic(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("topic")
	snap := s.deps.Snapshots.Snapshot()
	for _, t := range snap.Topics {
		if t.Name != name {
			continue
		}
		var parts []apiPartition
		for _, p := range t.Parts {
			parts = append(parts, apiPartition{
				Partition: p.Partition,
				Leader:    p.Leader,
				Replicas:  p.Replicas,
				ISR:       p.ISR,
				SizeBytes: p.SizeBytes,
				OffsetLag: p.OffsetLag,
				UnderRep:  p.UnderRep,
				Offline:   p.Offline,
			})
		}
		body := toAPIResponse(s, snap, &t)
		writeJSON(w, http.StatusOK, map[string]any{
			"topic":      body.Topics[0],
			"partitions": parts,
		})
		return
	}
	writeJSONErr(w, http.StatusNotFound, "unknown topic")
}

func toAPIResponse(s *Server, snap poller.Snapshot, only *poller.TopicView) apiResponse {
	resp := apiResponse{
		ClusterID:           snap.ClusterID,
		ControllerID:        snap.ControllerID,
		PolledAt:            snap.PolledAt.UTC(),
		PollOK:              snap.PollOK,
		LastError:           snap.LastError,
		StorageError:        snap.StorageError,
		SizeIncomplete:      snap.SizeIncomplete,
		MetadataUnavailable: snap.MetadataUnavailable,
		TotalBytes:          snap.TotalBytes,
		StaleAfterSeconds:   int64(s.deps.Snapshots.StaleThreshold().Seconds()),
		Brokers:             make([]apiBroker, 0, len(snap.Brokers)),
		BrokerErrors:        snap.BrokerErrors,
		Topics:              make([]apiTopic, 0, len(snap.Topics)),
	}
	for _, b := range snap.Brokers {
		resp.Brokers = append(resp.Brokers, apiBroker{
			ID: b.ID, Host: b.Host, Bytes: b.Bytes, Partitions: b.Partitions,
		})
	}
	for _, t := range snap.Topics {
		if only != nil && t.Name != only.Name {
			continue
		}
		resp.Topics = append(resp.Topics, apiTopic{
			Name:           t.Name,
			Partitions:     t.Partitions,
			Replicas:       t.Replicas,
			StorageBytes:   t.StorageBytes,
			LeaderBytes:    t.LeaderBytes,
			GrowthPerHour:  t.GrowthPerHour,
			PrevBytes:      t.PrevBytes,
			UnderRep:       t.UnderRep,
			Offline:        t.Offline,
			Stale:          t.Stale,
			ThresholdBytes: t.ThresholdBytes,
			WarnBytes:      t.WarnBytes,
			OverStreak:     t.OverStreak,
			ConfirmedOver:  t.ConfirmedOver,

			GrowthThreshold:  t.GrowthThreshold,
			GrowthStreak:     t.GrowthStreak,
			GrowthConfirmed:  t.GrowthConfirmed,
			HoursToLimit:     t.HoursToLimit,
			PartitionSkew:    t.PartitionSkew,
			RepStreak:        t.RepStreak,
			ReplicaConfirmed: t.ReplicaConfirmed,
		})
	}
	return resp
}

// handleAPIBrokers serves the per-broker storage view for automation (#12).
func (s *Server) handleAPIBrokers(w http.ResponseWriter, _ *http.Request) {
	snap := s.deps.Snapshots.Snapshot()
	var total int64
	for _, b := range snap.Brokers {
		total += b.Bytes
	}
	resp := apiBrokersResponse{
		ClusterID:        snap.ClusterID,
		PolledAt:         snap.PolledAt.UTC(),
		PollOK:           snap.PollOK,
		LastError:        snap.LastError,
		TotalBrokerBytes: total,
		Brokers:          make([]apiBrokerShare, 0, len(snap.Brokers)),
		SkewConfirmed:    snap.SkewConfirmed,
		SkewStreak:       snap.SkewStreak,
		SkewBrokerID:     snap.SkewBrokerID,
		SkewSharePct:     snap.SkewSharePct,
	}
	for _, b := range snap.Brokers {
		share := 0.0
		if total > 0 {
			share = float64(b.Bytes) / float64(total) * 100
		}
		resp.Brokers = append(resp.Brokers, apiBrokerShare{
			apiBroker: apiBroker{ID: b.ID, Host: b.Host, Bytes: b.Bytes, Partitions: b.Partitions},
			SharePct:  math.Round(share*10) / 10,
		})
	}
	writeJSON(w, http.StatusOK, resp)
}

type thresholdRequest struct {
	Topic          string `json:"topic"`
	ThresholdBytes int64  `json:"threshold_bytes"`
	WarnBytes      *int64 `json:"warn_bytes"`
	GrowthPerHour  *int64 `json:"growth_bytes_per_hour"`
}

func (s *Server) handleAPISetThreshold(w http.ResponseWriter, r *http.Request) {
	var body thresholdRequest
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeJSONErr(w, http.StatusBadRequest, "invalid JSON body")
		return
	}
	if err := s.setThreshold(r, body); err != nil {
		switch err {
		case errUnknownTopic:
			writeJSONErr(w, http.StatusNotFound, err.Error())
		default:
			writeJSONErr(w, http.StatusBadRequest, err.Error())
		}
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"topic":                 body.Topic,
		"threshold_bytes":       body.ThresholdBytes,
		"warn_bytes":            body.WarnBytes,
		"growth_bytes_per_hour": body.GrowthPerHour,
	})
}

func (s *Server) handleAPIDeleteThreshold(w http.ResponseWriter, r *http.Request) {
	topic := r.PathValue("topic")
	if err := s.deps.Store.DeleteThreshold(r.Context(), topic); err != nil {
		s.deps.Log.Error("delete threshold failed", "err", err)
		writeJSONErr(w, http.StatusInternalServerError, "store error")
		return
	}
	s.refreshThresholds(r)
	w.WriteHeader(http.StatusNoContent)
}

var errUnknownTopic = fmt.Errorf("unknown topic")

func (s *Server) setThreshold(r *http.Request, q thresholdRequest) error {
	if err := validateTopic(q.Topic); err != nil {
		return err
	}
	if q.ThresholdBytes <= 0 || q.ThresholdBytes > 1<<62 {
		return fmt.Errorf("threshold_bytes must be a positive integer")
	}
	if q.WarnBytes != nil && (*q.WarnBytes <= 0 || *q.WarnBytes >= q.ThresholdBytes) {
		return fmt.Errorf("warn_bytes must be positive and below threshold_bytes")
	}
	if q.GrowthPerHour != nil && *q.GrowthPerHour <= 0 {
		return fmt.Errorf("growth_bytes_per_hour must be a positive integer")
	}
	snap := s.deps.Snapshots.Snapshot()
	known := false
	for _, t := range snap.Topics {
		if t.Name == q.Topic {
			known = true
			break
		}
	}
	if !known {
		// Guard against typo'd topics: only topics the poller has observed
		// can carry a threshold.
		return errUnknownTopic
	}
	if err := s.deps.Store.SetThreshold(r.Context(), q.Topic, q.ThresholdBytes, q.WarnBytes, q.GrowthPerHour); err != nil {
		s.deps.Log.Error("set threshold failed", "err", err)
		return fmt.Errorf("store error")
	}
	s.refreshThresholds(r)
	return nil
}

func validateTopic(name string) error {
	if len(name) < 1 || len(name) > 249 {
		return fmt.Errorf("topic name must be 1-249 characters")
	}
	if strings.ContainsAny(name, " \t\n/") {
		return fmt.Errorf("topic name must not contain whitespace or '/'")
	}
	return nil
}

// joinInts renders a replica/ISR list as "1,2,3" for templates.
func joinInts(a []int32) string {
	parts := make([]string, 0, len(a))
	for _, v := range a {
		parts = append(parts, fmt.Sprintf("%d", v))
	}
	return strings.Join(parts, ",")
}

// replicationFactor renders average RF with one decimal for templates.
func replicationFactor(replicas, partitions int) string {
	if partitions <= 0 {
		return "-"
	}
	return fmt.Sprintf("%.1f", float64(replicas)/float64(partitions))
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeJSONErr(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}
