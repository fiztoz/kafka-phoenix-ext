package deploycheck

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func repoFile(t *testing.T, rel string) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("caller")
	}
	path := filepath.Join(filepath.Dir(file), "..", "..", rel)
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func repoRoot(t *testing.T) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("caller")
	}
	return filepath.Join(filepath.Dir(file), "..", "..")
}

func TestGrantsUseDedicatedSchemaNotTablePrefix(t *testing.T) {
	for _, rel := range []string{"README.md", "deploy/grants.sql", "docs/chart-wiring.md"} {
		for _, line := range strings.Split(repoFile(t, rel), "\n") {
			upper := strings.ToUpper(strings.TrimSpace(line))
			if strings.HasPrefix(upper, "--") || strings.HasPrefix(strings.TrimSpace(line), "#") {
				continue
			}
			if strings.Contains(upper, "GRANT ") && strings.Contains(line, "ext_kafka_usage_%") {
				t.Fatalf("%s still documents an invalid table-prefix grant: %s", rel, line)
			}
		}
	}
	grants := repoFile(t, "deploy/grants.sql")
	if !strings.Contains(grants, "ON kafka_usage.*") {
		t.Fatal("dedicated schema grant missing")
	}
	for _, table := range []string{
		"ext_kafka_usage_schema_migrations",
		"ext_kafka_usage_thresholds",
		"ext_kafka_usage_topic_state",
	} {
		if !strings.Contains(grants, table) {
			t.Fatalf("maintenance list missing %s", table)
		}
	}
	if strings.Contains(grants, "ON phoenix.*") || strings.Contains(grants, "ext_kafka_usage_%") {
		t.Fatal("grants still widen or use a prefix wildcard")
	}
	if !strings.Contains(grants, "utf8mb4_bin") {
		t.Fatal("dedicated schema must be case-sensitive")
	}
}

func TestReadinessProbeIsServingNotKafkaHealth(t *testing.T) {
	body := repoFile(t, "deploy/manifests/deployment.yaml")
	if strings.Contains(body, "path: /kafka/health/ready") || strings.Contains(body, "path: /health/ready") {
		t.Fatal("readiness probe still uses the Kafka dependency endpoint")
	}
	if strings.Count(body, "path: /kafka/health/live") < 3 {
		t.Fatal("startup, liveness, and readiness must all use /kafka/health/live")
	}
	wiring := repoFile(t, "docs/chart-wiring.md")
	if !strings.Contains(wiring, "readinessPath: /health/live") {
		t.Fatal("chart example must set readinessPath: /health/live")
	}
	if strings.Contains(wiring, "readinessPath can stay") {
		t.Fatal("chart docs still say the default readiness path is fine")
	}
}

func TestNetworkPolicySelectors(t *testing.T) {
	standalone := repoFile(t, "deploy/manifests/networkpolicy.yaml")
	chart := repoFile(t, "deploy/manifests/networkpolicy-chart.yaml")
	if !strings.Contains(standalone, "app.kubernetes.io/name: kafka-phoenix-ext") {
		t.Fatal("standalone policy must select the standalone deployment")
	}
	if selectorLinesContain(chart, "app.kubernetes.io/name: kafka-phoenix-ext") {
		t.Fatal("chart policy still selects the standalone name")
	}
	for _, want := range []string{
		"app.kubernetes.io/name: uptime-phoenix-ext",
		"app.kubernetes.io/instance: phoenix",
		"app.kubernetes.io/component: extension-kafka-usage",
		"port: 9092",
		"port: 9094",
	} {
		if !strings.Contains(chart, want) {
			t.Fatalf("chart policy missing %s", want)
		}
	}
	for _, want := range []string{"port: 53", "protocol: UDP", "protocol: TCP", "app.kubernetes.io/component: mariadb", "port: 3306"} {
		if !strings.Contains(standalone, want) {
			t.Fatalf("standalone policy missing %s", want)
		}
	}
	for _, body := range []string{standalone, chart} {
		for _, line := range strings.Split(body, "\n") {
			trimmed := strings.TrimSpace(line)
			if strings.HasPrefix(trimmed, "#") {
				continue
			}
			if strings.Contains(trimmed, "0.0.0.0/0") {
				t.Fatalf("egress allows every destination: %s", trimmed)
			}
		}
	}
}

func selectorLinesContain(body, needle string) bool {
	for _, line := range strings.Split(body, "\n") {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "#") {
			continue
		}
		if strings.Contains(line, needle) {
			return true
		}
	}
	return false
}
