// SASL wiring lives in its own file: credentials move from ClientOptions to
// the kgo mechanism exactly once and are never logged or returned.
package kafka

import (
	"fmt"

	"github.com/twmb/franz-go/pkg/sasl"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"
)

func saslMechanism(opts ClientOptions) (sasl.Mechanism, error) {
	switch opts.Mechanism {
	case "plain":
		return (plain.Auth{User: opts.Username, Pass: opts.Cred}).AsMechanism(), nil
	case "scram-sha-256":
		return (scram.Auth{User: opts.Username, Pass: opts.Cred}).AsSha256Mechanism(), nil
	case "scram-sha-512":
		return (scram.Auth{User: opts.Username, Pass: opts.Cred}).AsSha512Mechanism(), nil
	default:
		return nil, fmt.Errorf("kafka: unsupported SASL mechanism %q", opts.Mechanism)
	}
}
