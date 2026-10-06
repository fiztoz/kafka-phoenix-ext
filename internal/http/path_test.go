package http

import (
	"context"
	"net/url"
	"regexp"
	"strings"
	"testing"
)

func TestJoinPath(t *testing.T) {
	cases := []struct{ base, route, want string }{
		{"/", "/wallboard", "/wallboard"},
		{"/", "/", "/"},
		{"", "/topic/orders", "/topic/orders"},
		{"/kafka", "/wallboard", "/kafka/wallboard"},
		{"/kafka/", "/", "/kafka/"},
		{"/kafka/", "/thresholds", "/kafka/thresholds"},
	}
	for _, tc := range cases {
		if got := JoinPath(tc.base, tc.route); got != tc.want {
			t.Errorf("JoinPath(%q, %q) = %q, want %q", tc.base, tc.route, got, tc.want)
		}
	}
}

func TestReviewRootBasePathLinksRemainOnHost(t *testing.T) {
	assertLinksStayOnHost(t, "/", "http://localhost:8080/")
	assertLinksStayOnHost(t, "/kafka", "http://localhost:8080/kafka/")
}

func assertLinksStayOnHost(t *testing.T, base, pageURL string) {
	t.Helper()
	srv, _, st := newTestServerAt(t, sampleSnapshot(), "", base)
	pages := []string{
		JoinPath(base, "/"),
		JoinPath(base, "/wallboard"),
		JoinPath(base, "/topic/orders"),
		JoinPath(base, "/broker/0"),
	}
	var refs []string
	for _, page := range pages {
		w := do(t, srv.Handler(), "GET", page, nil, nil)
		if w.Code != 200 {
			t.Fatalf("GET %s: %d %s", page, w.Code, w.Body.String())
		}
		refs = append(refs, extractRefs(w.Body.String())...)
	}
	if len(refs) == 0 {
		t.Fatal("no links extracted")
	}
	baseURL, err := url.Parse(pageURL)
	if err != nil {
		t.Fatal(err)
	}
	sawWallboard, sawTopic, sawThresholds := false, false, false
	for _, ref := range refs {
		if strings.HasPrefix(ref, "?") || strings.HasPrefix(ref, "#") || ref == "" {
			continue
		}
		parsed, err := url.Parse(ref)
		if err != nil {
			t.Fatalf("parse %q: %v", ref, err)
		}
		got := baseURL.ResolveReference(parsed)
		if got.Hostname() != baseURL.Hostname() {
			t.Fatalf("ref %q from %s resolved to %s", ref, pageURL, got.String())
		}
		if strings.Contains(ref, "wallboard") {
			sawWallboard = true
		}
		if strings.Contains(ref, "/topic/orders") {
			sawTopic = true
		}
		if strings.Contains(ref, "/thresholds") {
			sawThresholds = true
		}
		if strings.HasPrefix(ref, "//") {
			t.Fatalf("network-path reference %q", ref)
		}
	}
	if !sawWallboard || !sawTopic || !sawThresholds {
		t.Fatalf("missing expected refs wallboard=%v topic=%v thresholds=%v from %v", sawWallboard, sawTopic, sawThresholds, refs)
	}

	if err := st.SetThreshold(context.Background(), "orders", 10, nil, nil); err != nil {
		t.Fatal(err)
	}
	form := "topic=orders&threshold_bytes=10"
	w := do(t, srv.Handler(), "POST", JoinPath(base, "/thresholds"),
		map[string]string{"Content-Type": "application/x-www-form-urlencoded"}, strings.NewReader(form))
	assertRedirectOnHost(t, baseURL, w.Code, w.Header().Get("Location"))
	w = do(t, srv.Handler(), "POST", JoinPath(base, "/thresholds/delete"),
		map[string]string{"Content-Type": "application/x-www-form-urlencoded"}, strings.NewReader("topic=orders"))
	assertRedirectOnHost(t, baseURL, w.Code, w.Header().Get("Location"))
}

func assertRedirectOnHost(t *testing.T, base *url.URL, code int, location string) {
	t.Helper()
	if code != 303 {
		t.Fatalf("redirect status = %d", code)
	}
	parsed, err := url.Parse(location)
	if err != nil {
		t.Fatal(err)
	}
	got := base.ResolveReference(parsed)
	if got.Hostname() != base.Hostname() {
		t.Fatalf("redirect %q resolved to %s", location, got.String())
	}
	if strings.HasPrefix(location, "//") {
		t.Fatalf("redirect is a network-path reference: %s", location)
	}
}

var refPatterns = []*regexp.Regexp{
	regexp.MustCompile(`href="([^"]+)"`),
	regexp.MustCompile(`action="([^"]+)"`),
	regexp.MustCompile(`formaction="([^"]+)"`),
}

func extractRefs(body string) []string {
	var out []string
	for _, re := range refPatterns {
		for _, m := range re.FindAllStringSubmatch(body, -1) {
			out = append(out, m[1])
		}
	}
	return out
}
