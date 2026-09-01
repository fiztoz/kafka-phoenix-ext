package http

import "strings"

// errKind buckets a failed poll so the UI and /health/ready can say what
// actually went wrong. "Kafka is down" and "this principal lacks ACLs" need
// different on-call responses; the raw error string alone buries that.
type errKind string

const (
	errAuth    errKind = "auth"
	errTimeout errKind = "timeout"
	errNetwork errKind = "network"
)

// classifyErr inspects the poller's last error string. franz-go surfaces
// broker auth failures with the Kafka error name in the message
// (CLUSTER_AUTHORIZATION_FAILED, TOPIC_AUTHORIZATION_FAILED, ...), which is
// stable enough to match on without importing the kerr table here.
func classifyErr(msg string) errKind {
	up := strings.ToUpper(msg)
	switch {
	case strings.Contains(up, "AUTHORIZATION"), strings.Contains(up, "UNAUTHORIZED"):
		return errAuth
	case strings.Contains(msg, "deadline"), strings.Contains(up, "TIMED OUT"), strings.Contains(up, "TIMEOUT"):
		return errTimeout
	default:
		return errNetwork
	}
}

// authHint documents the ACLs the storage poller needs, shown next to an
// auth-classified failure so the operator can fix the principal in place.
const authHint = "SASL handshake succeeded, but this principal lacks ACLs. " +
	"Required: CLUSTER Describe, TOPIC Describe (used by DescribeLogDirs). " +
	"Consumer-group features additionally need GROUP Describe/Read."
