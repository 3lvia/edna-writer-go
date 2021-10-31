package sink

import (
	"cloud.google.com/go/bigquery"
	"context"
	"fmt"
	"github.com/3lvia/metrics-go/metrics"
)

const (
	metricsTemplateReceived = `sink_%s_received`
	metricsTemplateFlushed = `sink_%s_flushed`
	metricsErrors = `sink_errors`
)

type streamHandler struct {
	client     *bigquery.Client
	dataset    string
	operations TableOperations
	metrics    metrics.Metrics
}

func (s *streamHandler) start(ctx context.Context, stream targetStream, errorOutput chan<- error) {
	metricsReceived := fmt.Sprintf(metricsTemplateReceived, stream.Type())
	metricsFlushed := fmt.Sprintf(metricsTemplateFlushed, stream.Type())

	var rows []bigquery.ValueSaver
	for {
		select {
		case obj := <-stream.Stream():
			rows = append(rows, obj)
			s.metrics.IncCounter(metricsReceived, metrics.DayLabels())
		case <-stream.Done():
			err := s.operations.Write(ctx, s.client, s.dataset, stream.Schema(), rows)
			if err != nil {
				errorOutput <- err
				s.metrics.IncCounter(metricsErrors, metrics.DayLabels())

				rows = []bigquery.ValueSaver{}
				continue
			}
			c := s.metrics.Counter(metricsFlushed, metrics.DayLabels())
			c.Add(float64(len(rows)))
			rows = []bigquery.ValueSaver{}
		}
	}
}
