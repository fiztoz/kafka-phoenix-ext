package main

import (
	"github.com/fiztoz/kafka-phoenix-ext/internal/config"
	"github.com/fiztoz/kafka-phoenix-ext/internal/kafka"
)

// pollerOptions derives one cluster's describe filter from its config.
func pollerOptions(c config.Cluster) kafka.DescribeOptions {
	return kafka.DescribeOptions{
		IncludeInternal: c.IncludeInternal,
		TopicAllowed:    c.TopicAllowed,
	}
}
