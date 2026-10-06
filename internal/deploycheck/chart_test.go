package deploycheck

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestChartExampleWiresDatabase(t *testing.T) {
	wiring := repoFile(t, "docs/chart-wiring.md")
	if !strings.Contains(wiring, "secretName: kafka-phoenix-ext-db") {
		t.Fatal("chart example missing database secret name")
	}
	if !strings.Contains(wiring, "secretKey: dsn") {
		t.Fatal("chart example missing database secret key")
	}
	if !strings.Contains(wiring, "name: KAFKA_SASL_PASSWORD") {
		t.Fatal("chart example missing SASL env name")
	}
	if !strings.Contains(wiring, "key: sasl-password") {
		t.Fatal("chart example missing SASL secret key")
	}
	if strings.Contains(wiring, "envFromSecret: kafka-phoenix-ext") {
		t.Fatal("example uses envFromSecret with a key that is not the env var name")
	}
}

func TestHelmRendersChartSecretRefs(t *testing.T) {
	helm, err := exec.LookPath("helm")
	if err != nil {
		t.Skip("helm not installed")
	}
	chart := filepath.Join(repoRoot(t), "..", "uptime-phoenix", "charts", "uptime-phoenix")
	if _, err := os.Stat(chart); err != nil {
		t.Skip("uptime-phoenix chart not available")
	}
	values := filepath.Join(t.TempDir(), "values.yaml")
	example := extractYAMLFence(repoFile(t, "docs/chart-wiring.md"))
	if !strings.HasPrefix(strings.TrimSpace(example), "extensions:") {
		t.Fatal("chart example fence is not values YAML")
	}
	if err := os.WriteFile(values, []byte(example), 0o600); err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(helm, "template", "phoenix", chart, "-f", values, "--set", "networkPolicy.enabled=true")
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("helm template: %v\n%s", err, out)
	}
	rendered := string(out)
	for _, want := range []string{
		"DATABASE_DSN",
		"kafka-phoenix-ext-db",
		"dsn",
		"KAFKA_SASL_PASSWORD",
		"sasl-password",
		"/health/live",
		"app.kubernetes.io/name: uptime-phoenix-ext",
		"app.kubernetes.io/instance: phoenix",
		"app.kubernetes.io/component: extension-kafka-usage",
	} {
		if !strings.Contains(rendered, want) {
			t.Fatalf("rendered chart missing %q", want)
		}
	}
	policy := repoFile(t, "deploy/manifests/networkpolicy-chart.yaml")
	for _, label := range []string{
		"app.kubernetes.io/name: uptime-phoenix-ext",
		"app.kubernetes.io/instance: phoenix",
		"app.kubernetes.io/component: extension-kafka-usage",
	} {
		if !strings.Contains(policy, label) || !strings.Contains(rendered, label) {
			t.Fatalf("chart policy selector %s does not match rendered pod labels", label)
		}
	}
}

func extractYAMLFence(md string) string {
	const start = "```yaml\n"
	i := strings.Index(md, start)
	if i < 0 {
		return ""
	}
	rest := md[i+len(start):]
	j := strings.Index(rest, "```")
	if j < 0 {
		return ""
	}
	return rest[:j]
}
