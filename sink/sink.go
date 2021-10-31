// Package sink provides a way for clients to write data to BigQuery through the usage of a streaming metaphor.
//
// It is the responsibility of the client code to provide the schema and other specifics of the BigQuery dataset and
// table, and then this package handles the actual writing of data.
//
// Currently, real streaming is not implemented, and this package rather caches rows in memory before flushing to
// BigQuery when the calling code sends a signal.
package sink

import (
	"cloud.google.com/go/bigquery"
	"context"
	"errors"
	"fmt"
	"github.com/3lvia/metrics-go/metrics"
	"log"
)

var streams []targetStream

// Start the internal functionality by creating and starting an internal handler per incoming stream. Each handler is
// started in a separate go routine.
func Start(ctx context.Context, opts ...Option) {
	if len(streams) == 0 {
		err :=  errors.New("at least one stream must be registered before starting this module")
		log.Fatal(err)
	}

	collector := &optionsCollector{}
	for _, opt := range opts {
		opt(collector)
	}

	errorChan := make(chan error)

	ops := collector.operations(ctx)

	for _, stream := range streams {
		handler := &streamHandler{
			dataset:    collector.datasetID,
			operations: ops,
			metrics:    collector.metrics,
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

// Stream creates and returns a stream that client code chan be used to stream objects that shall be written to
// BigQuery. This function has the side effect of caching the corresponding target stream internally so that changes
// are handled when this package is started.
func Stream(typ string, schema Schema) SourceStream {
	s := &stream{
		typ:     typ,
		schema:  schema,
		objects: make(chan bigquery.ValueSaver),
		done:    make(chan struct{}),
	}
	streams = append(streams, s)
	return s
}