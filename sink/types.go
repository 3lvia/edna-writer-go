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

type SourceStream interface {
	Type() string
	Send(v bigquery.ValueSaver)
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
