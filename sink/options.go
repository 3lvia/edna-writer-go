package sink

import (
	"cloud.google.com/go/bigquery"
	"context"
	"github.com/3lvia/hn-config-lib-go/vault"
	"github.com/3lvia/metrics-go/metrics"
	"log"
)

type optionsCollector struct {
	projectID string
	datasetID string
	ops       TableOperations
	errorChan chan error

	v vault.SecretsManager

	metrics metrics.Metrics
}

func (c *optionsCollector) operations(ctx context.Context) TableOperations {
	if c.ops != nil {
		return c.ops
	}

	client, err := bigquery.NewClient(ctx, c.projectID)
	if err != nil {
		log.Fatal(err)
	}
	return &tableOperations{client: client}
}

// Option for configuring this package.
type Option func(collector *optionsCollector)

// WithBigQuery initializes this package with the information needed to access Google BigQuery.
func WithBigQuery(projectID, datasetID string) Option {
	return func(collector *optionsCollector) {
		collector.projectID = projectID
		collector.datasetID = datasetID
	}
}

// WithMetrics initializes this package with the metrics service.
func WithMetrics(m metrics.Metrics) Option {
	return func(collector *optionsCollector) {
		collector.metrics = m
	}
}

// WithTableOperations sets the interface that is used to write to BigQuery internally. The point is to provide a way
// by which this package can be unit tested. This function should not be used in production.
func WithTableOperations(op TableOperations) Option {
	return func(collector *optionsCollector) {
		collector.ops = op
	}
}

// WithErrorChannel sets a channel that this module will use to communicate all errors out.
func WithErrorChannel(errChan chan error) Option {
	return func(collector *optionsCollector) {
		collector.errorChan = errChan
	}
}

// WithVault sets the vault secrets manager to be used. This is used for obtaining the Google Cloud service account that
// must be set in order to access BigQuery. If this option is not used, this module will assume that the environment
// variable GOOGLE_APPLICATION_CREDENTIALS has already been set by the client code.
func WithVault(v vault.SecretsManager) Option {
	return func(collector *optionsCollector) {
		collector.v = v
	}
}