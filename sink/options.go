package sink

import "github.com/3lvia/metrics-go/metrics"

type optionsCollector struct {
	projectID string
	datasetID string
	ops       TableOperations

	metrics metrics.Metrics
}

func (c *optionsCollector) operations() TableOperations {
	if c.ops == nil {
		return &tableOperations{}
	}
	return c.ops
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