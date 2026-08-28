// Package kafka speaks to the Kafka cluster via the franz-go admin client
// (kadm) and aggregates disk usage per topic, per partition and per broker.
package kafka

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

// ClientOptions is everything the client needs to dial the cluster.
// Credentials flow env → ClientWiring → kgo; they are never logged.
type ClientOptions struct {
	Brokers   []string
	Timeout   time.Duration // per-request timeout applied to each poll
	TLS       bool          // dial with TLS (KAFKA_TLS_ENABLED)
	CAFile    string        // PEM file on disk; takes precedence over CAInline
	CAInline  string        // inline PEM
	Mechanism string        // none | plain | scram-sha-256 | scram-sha-512
	Username  string
	Cred      string
	Logger    func(msg string, args ...any)
}

// Client wraps a franz-go kgo client and its kadm admin surface.
type Client struct {
	kgo  *kgo.Client
	adm  *kadm.Client
	opts ClientOptions
}

// NewClient builds the kgo client. No network I/O happens here; dialing is
// lazy and only occurs on the first describe request.
func NewClient(opts ClientOptions) (*Client, error) {
	kgoOpts := []kgo.Opt{
		kgo.SeedBrokers(opts.Brokers...),
		kgo.ClientID("kafka-phoenix-ext"),
		kgo.DialTimeout(10 * time.Second),
		kgo.MetadataMaxAge(5 * time.Minute),
		// Auto topic creation stays disabled (franz-go default); this
		// extension is strictly read-only.
	}

	dsn := "plaintext"
	if opts.TLS {
		tlsCfg, err := buildTLS(opts)
		if err != nil {
			return nil, fmt.Errorf("kafka: %w", err)
		}
		kgoOpts = append(kgoOpts, kgo.DialTLSConfig(tlsCfg))
		dsn = "tls"
	}

	if opts.Mechanism != "" && opts.Mechanism != "none" {
		mech, err := saslMechanism(opts)
		if err != nil {
			return nil, err
		}
		kgoOpts = append(kgoOpts, kgo.SASL(mech))
		dsn += "/sasl/" + opts.Mechanism
	}

	cl, err := kgo.NewClient(kgoOpts...)
	if err != nil {
		return nil, fmt.Errorf("kafka: new client: %w", err)
	}
	if opts.Logger != nil {
		opts.Logger("kafka client created", "dsn", dsn, "brokers", strings.Join(opts.Brokers, ","))
	}
	return &Client{kgo: cl, adm: kadm.NewClient(cl), opts: opts}, nil
}

// Close shuts the kgo client down.
func (c *Client) Close() {
	c.kgo.Close()
}

func buildTLS(opts ClientOptions) (*tls.Config, error) {
	cfg := &tls.Config{
		MinVersion: tls.VersionTLS12,
		ServerName: "", // kgo derives it per broker from the seed hostname
	}
	ca := opts.CAInline
	if opts.CAFile != "" {
		pem, err := os.ReadFile(opts.CAFile)
		if err != nil {
			return nil, fmt.Errorf("kafka: read TLS CA file: %w", err)
		}
		ca = string(pem)
	}
	if strings.TrimSpace(ca) == "" {
		return cfg, nil // system pool
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM([]byte(ca)) {
		return nil, fmt.Errorf("kafka: KAFKA_TLS_CA contains no valid PEM certificates")
	}
	cfg.RootCAs = pool
	return cfg, nil
}
