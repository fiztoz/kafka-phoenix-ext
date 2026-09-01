package http

import (
	"fmt"
	"net/http"
	"sort"
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

	Sort             string // active dashboard sort: size (default), growth, skew, name
	ErrKind          string // auth | timeout | network when PollOK is false
	ErrHint          string // required-ACL guidance for auth failures
	SkewConfirmed    bool
	SkewHotID        int32
	SkewShare        string // hottest broker's share, e.g. "64.2%"
	TotalBrokerBytes int64
}

// --- dashboards ---

func (s *Server) handleDashboard(w http.ResponseWriter, r *http.Request) {
	pd := s.dashboardData()
	pd.Sort = sortKey(r)
	pd.Topics = sortTopics(pd.Topics, pd.Sort)
	s.render(w, "dashboard.html", pd)
}

// handleWallboard orders by severity first (confirmed problems on top),
// then size: a wallboard earns its keep when the red tiles sit together.
func (s *Server) handleWallboard(w http.ResponseWriter, _ *http.Request) {
	pd := s.dashboardData()
	pd.Topics = wallboardOrder(pd.Topics)
	s.render(w, "wallboard.html", pd)
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

// hostedTopicRow is one topic with replica copies on the drilled-down broker.
type hostedTopicRow struct {
	Name        string
	Copies      int
	LeaderParts int
	LeaderBytes int64
}

// leaderPartitionRow is one partition led by the drilled-down broker.
type leaderPartitionRow struct {
	Topic     string
	Partition int32
	SizeBytes int64
}

// handleBrokerPage renders the per-broker drill-down (#9): what the broker
// holds, computed from the same DescribeLogDirs + Metadata payload the
// poller already paid for. No extra Kafka calls.
func (s *Server) handleBrokerPage(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseInt(r.PathValue("id"), 10, 32)
	if err != nil {
		http.Error(w, "unknown broker", http.StatusNotFound)
		return
	}
	snap := s.deps.Snapshots.Snapshot()
	var broker *apiBroker
	var totalBrokerBytes int64
	for _, b := range snap.Brokers {
		totalBrokerBytes += b.Bytes
		if b.ID == int32(id) {
			broker = &apiBroker{ID: b.ID, Host: b.Host, Bytes: b.Bytes, Partitions: b.Partitions}
		}
	}
	if broker == nil {
		http.Error(w, "unknown broker", http.StatusNotFound)
		return
	}

	hosted := map[string]*hostedTopicRow{}
	var leaders []leaderPartitionRow
	var leaderParts int
	var leaderBytes int64
	for _, t := range snap.Topics {
		for _, pu := range t.Parts {
			onBroker := pu.Leader == int32(id)
			for _, rp := range pu.Replicas {
				if rp == int32(id) {
					onBroker = true
					break
				}
			}
			if !onBroker {
				continue
			}
			row := hosted[t.Name]
			if row == nil {
				row = &hostedTopicRow{Name: t.Name}
				hosted[t.Name] = row
			}
			row.Copies++
			if pu.Leader == int32(id) {
				row.LeaderParts++
				row.LeaderBytes += pu.SizeBytes
				leaderParts++
				leaderBytes += pu.SizeBytes
				leaders = append(leaders, leaderPartitionRow{Topic: t.Name, Partition: pu.Partition, SizeBytes: pu.SizeBytes})
			}
		}
	}
	hostedRows := make([]hostedTopicRow, 0, len(hosted))
	for _, row := range hosted {
		hostedRows = append(hostedRows, *row)
	}
	sort.Slice(hostedRows, func(i, j int) bool {
		if hostedRows[i].LeaderBytes != hostedRows[j].LeaderBytes {
			return hostedRows[i].LeaderBytes > hostedRows[j].LeaderBytes
		}
		return hostedRows[i].Name < hostedRows[j].Name
	})
	sort.Slice(leaders, func(i, j int) bool {
		if leaders[i].SizeBytes != leaders[j].SizeBytes {
			return leaders[i].SizeBytes > leaders[j].SizeBytes
		}
		return leaders[i].Topic < leaders[j].Topic
	})
	const maxLeaderRows = 25
	leaderTruncated := len(leaders) > maxLeaderRows
	if leaderTruncated {
		leaders = leaders[:maxLeaderRows]
	}

	share := "0.0%"
	if totalBrokerBytes > 0 {
		share = fmt.Sprintf("%.1f%%", float64(broker.Bytes)/float64(totalBrokerBytes)*100)
	}

	data := struct {
		pageData
		Broker          apiBroker
		Share           string
		Hosted          []hostedTopicRow
		Leaders         []leaderPartitionRow
		LeaderParts     int
		LeaderBytes     int64
		LeaderTruncated bool
	}{
		pageData:        s.dashboardData(),
		Broker:          *broker,
		Share:           share,
		Hosted:          hostedRows,
		Leaders:         leaders,
		LeaderParts:     leaderParts,
		LeaderBytes:     leaderBytes,
		LeaderTruncated: leaderTruncated,
	}
	s.render(w, "broker.html", data)
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

		SkewConfirmed: snap.SkewConfirmed,
		SkewHotID:     snap.SkewBrokerID,
		SkewShare:     fmt.Sprintf("%.1f%%", snap.SkewSharePct),
	}
	for _, b := range snap.Brokers {
		pd.TotalBrokerBytes += b.Bytes
		pd.Brokers = append(pd.Brokers, apiBroker{
			ID: b.ID, Host: b.Host, Bytes: b.Bytes, Partitions: b.Partitions,
		})
	}
	if !snap.PollOK {
		pd.ErrKind = string(classifyErr(snap.LastError))
		if pd.ErrKind == string(errAuth) {
			pd.ErrHint = authHint
		}
	}
	return pd
}

// sortKey whitelists the dashboard sort query param.
func sortKey(r *http.Request) string {
	switch r.URL.Query().Get("sort") {
	case "growth", "skew", "name":
		return r.URL.Query().Get("sort")
	default:
		return "size"
	}
}

func sortTopics(ts []poller.TopicView, key string) []poller.TopicView {
	out := append([]poller.TopicView{}, ts...)
	switch key {
	case "name":
		sort.SliceStable(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	case "growth":
		sort.SliceStable(out, func(i, j int) bool {
			if out[i].GrowthPerHour != out[j].GrowthPerHour {
				return out[i].GrowthPerHour > out[j].GrowthPerHour
			}
			return out[i].StorageBytes > out[j].StorageBytes
		})
	case "skew":
		sort.SliceStable(out, func(i, j int) bool {
			if out[i].PartitionSkew != out[j].PartitionSkew {
				return out[i].PartitionSkew > out[j].PartitionSkew
			}
			return out[i].StorageBytes > out[j].StorageBytes
		})
	default: // size
		sort.SliceStable(out, func(i, j int) bool {
			if out[i].StorageBytes != out[j].StorageBytes {
				return out[i].StorageBytes > out[j].StorageBytes
			}
			return out[i].Name < out[j].Name
		})
	}
	return out
}

// wallboardOrder puts confirmed problems first, then warnings, then the rest
// by size, so a glance at the top of the wall is a triage.
func wallboardOrder(ts []poller.TopicView) []poller.TopicView {
	out := append([]poller.TopicView{}, ts...)
	rank := func(t poller.TopicView) int {
		switch {
		case t.ConfirmedOver || t.ReplicaConfirmed:
			return 0
		case t.GrowthConfirmed:
			return 1
		case t.ThresholdBytes != nil && t.WarnBytes != nil && t.StorageBytes >= numValue(t.WarnBytes):
			return 2
		case t.UnderRep > 0 || t.Offline > 0:
			return 3
		default:
			return 4
		}
	}
	sort.SliceStable(out, func(i, j int) bool {
		ri, rj := rank(out[i]), rank(out[j])
		if ri != rj {
			return ri < rj
		}
		return out[i].StorageBytes > out[j].StorageBytes
	})
	return out
}

// --- threshold forms ---

// handleThresholdForm accepts a plain HTML form post (topic page actions)
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
	var growth *int64
	if gS := r.Form.Get("growth_bytes_per_hour"); gS != "" {
		v, err := strconv.ParseInt(gS, 10, 64)
		if err != nil {
			http.Error(w, "growth threshold must be an integer number of bytes per hour", http.StatusBadRequest)
			return
		}
		growth = &v
	}
	if err := s.setThreshold(r, thresholdRequest{Topic: topic, ThresholdBytes: limit, WarnBytes: warn, GrowthPerHour: growth}); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	redirect()
}

// handleThresholdDeleteForm removes a threshold from the UI.
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

// Growth renders a bytes/hour rate with sign, e.g. "+3.2 MiB/hr". A zero
// rate renders as the word "flat": it means the window saw no net change,
// not that the value is missing.
func Growth(perHour int64) string {
	if perHour == 0 {
		return "flat"
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

// partSkew renders the max/avg partition-size ratio as "2.4x", or "-" when
// the topic has too few sized partitions to say anything.
func partSkew(f float64) string {
	if f <= 0 {
		return "-"
	}
	return fmt.Sprintf("%.1fx", f)
}

func ts(t time.Time) string {
	if t.IsZero() {
		return "n/a"
	}
	return t.UTC().Format("2006-01-02 15:04 UTC")
}

func age(t time.Time) string {
	if t.IsZero() {
		return "n/a"
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
