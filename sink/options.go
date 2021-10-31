package sink

import "github.com/3lvia/metrics-go/metrics"

type optionsCollector struct {
	projectID      string
	datasetID      string

	metrics metrics.Metrics
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
