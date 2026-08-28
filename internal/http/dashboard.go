package http

import (
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/fiztoz/kafka-phoenix-ext/internal/poller"
)

// --- page data ---

type pageData struct {
	BasePath     string
	ClusterID    string
	ControllerID int32
	Brokers      []apiBroker
	BrokerErrors []string
	PolledAt     time.Time
	PollOK       bool
	LastError    string
	TotalBytes   int64
	Topics       []poller.TopicView
}

// --- dashboards ---

func (s *Server) handleDashboard(w http.ResponseWriter, _ *http.Request) {
	s.render(w, "dashboard.html", s.dashboardData())
}

func (s *Server) handleWallboard(w http.ResponseWriter, _ *http.Request) {
	s.render(w, "wallboard.html", s.dashboardData())
}

func (s *Server) handleTopicPage(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	snap := s.deps.Snapshots.Snapshot()
	var topic *poller.TopicView
	for i := range snap.Topics {
		if snap.Topics[i].Name == name {
			topic = &snap.Topics[i]
			break
		}
	}
	if topic == nil {
		// Keep the 404 sparse: no cluster topology leaks.
		http.Error(w, "unknown topic", http.StatusNotFound)
		return
	}
	data := struct {
		pageData
		Topic *poller.TopicView
	}{
		pageData: s.dashboardData(),
		Topic:    topic,
	}
	s.render(w, "topic.html", data)
}

func (s *Server) render(w http.ResponseWriter, name string, data any) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := s.tmpl.ExecuteTemplate(w, name, data); err != nil {
		s.deps.Log.Error("render failed", "template", name, "err", err)
	}
}

// dashboardData assembles the template payload.
func (s *Server) dashboardData() pageData {
	snap := s.deps.Snapshots.Snapshot()
	pd := pageData{
		BasePath:     s.deps.BasePath,
		ClusterID:    snap.ClusterID,
		ControllerID: snap.ControllerID,
		BrokerErrors: snap.BrokerErrors,
		PolledAt:     snap.PolledAt,
		PollOK:       snap.PollOK,
		LastError:    snap.LastError,
		TotalBytes:   snap.TotalBytes,
		Topics:       snap.Topics,
		Brokers:      make([]apiBroker, 0, len(snap.Brokers)),
	}
	for _, b := range snap.Brokers {
		pd.Brokers = append(pd.Brokers, apiBroker{
			ID: b.ID, Host: b.Host, Bytes: b.Bytes, Partitions: b.Partitions,
		})
	}
	return pd
}

// --- threshold forms ---

// handleThresholdForm accepts a plain HTML form post (dashboard actions)
// and redirects back to the dashboard.
func (s *Server) handleThresholdForm(w http.ResponseWriter, r *http.Request) {
	redirect := func() {
		http.Redirect(w, r, s.deps.BasePath+"/", http.StatusSeeOther)
	}
	if err := r.ParseForm(); err != nil {
		http.Error(w, "invalid form", http.StatusBadRequest)
		return
	}
	topic := r.Form.Get("topic")
	limit, err := strconv.ParseInt(r.Form.Get("threshold_bytes"), 10, 64)
	if err != nil {
		http.Error(w, "threshold must be an integer number of bytes", http.StatusBadRequest)
		return
	}
	var warn *int64
	if wS := r.Form.Get("warn_bytes"); wS != "" {
		v, err := strconv.ParseInt(wS, 10, 64)
		if err != nil {
			http.Error(w, "warn threshold must be an integer number of bytes", http.StatusBadRequest)
			return
		}
		warn = &v
	}
	if err := s.setThreshold(r, thresholdRequest{Topic: topic, ThresholdBytes: limit, WarnBytes: warn}); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	redirect()
}

// handleThresholdDeleteForm removes a threshold from the dashboard UI.
func (s *Server) handleThresholdDeleteForm(w http.ResponseWriter, r *http.Request) {
	redirect := func() {
		http.Redirect(w, r, s.deps.BasePath+"/", http.StatusSeeOther)
	}
	if err := r.ParseForm(); err != nil {
		http.Error(w, "invalid form", http.StatusBadRequest)
		return
	}
	topic := r.Form.Get("topic")
	if err := validateTopic(topic); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if err := s.deps.Store.DeleteThreshold(r.Context(), topic); err != nil {
		http.Error(w, "store error", http.StatusInternalServerError)
		return
	}
	s.refreshThresholds(r)
	redirect()
}

// --- units / formatting helpers for templates ---

// HumanBytes renders a byte count in binary units (GiB/TiB), matching how
// Kafka log dir sizes are reported.
func HumanBytes(n int64) string {
	abs := n
	sign := ""
	if n < 0 {
		sign = "-"
		abs = -n
	}
	const unit = 1024
	if abs < unit {
		return sign + fmt.Sprintf("%d B", abs)
	}
	div, exp := int64(unit), 0
	for m := abs / unit; m >= unit; m /= unit {
		div *= unit
		exp++
	}
	return sign + fmt.Sprintf("%.1f %ciB", float64(abs)/float64(div), "KMGTPE"[exp])
}

// Growth renders a bytes/hour rate with sign, e.g. "+3.2 MiB/hr".
func Growth(perHour int64) string {
	if perHour == 0 {
		return "—"
	}
	sign := "+"
	if perHour < 0 {
		sign = "-"
		perHour = -perHour
	}
	return fmt.Sprintf("%s %s/hr", sign, HumanBytes(perHour))
}

func pct(part, whole int64) string {
	if whole <= 0 {
		return "0.0%"
	}
	return fmt.Sprintf("%.1f%%", float64(part)/float64(whole)*100)
}

// formatNum renders a nullable int64 for form prefill.
func formatNum(p *int64) string {
	if p == nil {
		return ""
	}
	return strconv.FormatInt(*p, 10)
}

// numValue dereferences a nullable int64, returning 0 for nil.
func numValue(p *int64) int64 {
	if p == nil {
		return 0
	}
	return *p
}

func ts(t time.Time) string {
	if t.IsZero() {
		return "—"
	}
	return t.UTC().Format("2006-01-02 15:04 UTC")
}

func age(t time.Time) string {
	if t.IsZero() {
		return "—"
	}
	d := time.Since(t)
	switch {
	case d < time.Minute:
		return fmt.Sprintf("%ds", int(d.Seconds()))
	case d < 60*time.Minute:
		return fmt.Sprintf("%dm", int(d.Minutes()))
	default:
		return fmt.Sprintf("%dh%dm", int(d.Hours()), int(d.Minutes())%60)
	}
}
