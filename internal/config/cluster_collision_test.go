package config

import (
	"strings"
	"testing"
)

func TestClusterIDsRejectEnvironmentPrefixCollision(t *testing.T) {
	for _, list := range []string{"prod-eu,prod_eu", "PROD_EU,prod-eu"} {
		t.Run(list, func(t *testing.T) {
			_, err := parseClusterIDs(list)
			if err == nil || !strings.Contains(err.Error(), "KAFKA_PROD_EU_") {
				t.Fatalf("ambiguous environment prefix must fail: %v", err)
			}
		})
	}
	if _, err := parseClusterIDs("harbor,summit"); err != nil {
		t.Fatalf("distinct prefixes rejected: %v", err)
	}
}
