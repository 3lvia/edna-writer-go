package sink

import "cloud.google.com/go/bigquery"

// Schema wraps the BigQuery schema and write disposition.
type Schema struct {
	BQSchema         *bigquery.TableMetadata
	Disposition      bigquery.TableWriteDisposition
}

type targetStream interface {
	Type() string
	Stream() <-chan bigquery.ValueSaver
	Done() <-chan struct{}
	Schema() Schema
}

// SourceStream is the stream that the source of the data that shall be written to BigQuery uses in order to communicate
// with this package.
type SourceStream interface {
	// Type is the type of the stream, usually the same as the table that the data is written to in BigQuery.
	Type() string

	// Send sends the given value on the stream.
	Send(v bigquery.ValueSaver)

	// Complete sends the signal that the stream is now complete for this iteration to the receiver.
	Complete()
}

type stream struct {
	typ    string
	schema Schema

	objects chan bigquery.ValueSaver
	done    chan struct{}
}

func (s *stream) Type() string {
	return s.typ
}

func (s *stream) Schema() Schema {
	return s.schema
}

func (s *stream) Stream() <-chan bigquery.ValueSaver {
	return s.objects
}

func (s *stream) Send(v bigquery.ValueSaver) {
	s.objects <- v
}

func (s *stream) Done() <-chan struct{} {
	return s.done
}

func (s *stream) Complete() {
	s.done <- struct{}{}
}
