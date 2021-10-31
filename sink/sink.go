package sink

import (
	"cloud.google.com/go/bigquery"
	"context"
	"fmt"
	"github.com/3lvia/edna-writer/types"
	"github.com/3lvia/metrics-go/metrics"
	"log"
)

// Start the internal functionality by creating and starting an internal handler per incoming stream. Each handler is
// started in a separate go routine.
func Start(ctx context.Context, streams []types.TargetStream, opts ...Option) {
	collector := &optionsCollector{}
	for _, opt := range opts {
		opt(collector)
	}

	client, err := bigquery.NewClient(ctx, collector.projectID)
	if err != nil {
		log.Fatal(err)
	}
	errorChan := make(chan error)

	for _, stream := range streams {
		handler := &streamHandler{
			client:   client,
			dataset:  collector.datasetID,
			metrics:  collector.metrics,
		}
		go handler.start(ctx, stream, errorChan)
	}

	go func(ec <-chan error) {
		for {
			e := <- ec
			collector.metrics.IncCounter("sink_errors", metrics.DayLabels())
			log.Print(fmt.Sprintf("%v", e))
		}
	}(errorChan)
}