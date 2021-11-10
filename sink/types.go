package sink

import "cloud.google.com/go/bigquery"

// Schema wraps the BigQuery schema and write disposition.
type Schema struct {
	BQSchema         *bigquery.TableMetadata
	Disposition      bigquery.TableWriteDisposition
}

// SourceStream is the streamImpl that the source of the data that shall be written to BigQuery uses in order to communicate
// with this package.
type SourceStream interface {
	// Type is the type of the stream, usually the same as the table that the data is written to in BigQuery.
	Type() string

	// Send sends the given value on the stream.
	Send(v bigquery.ValueSaver)

	// SendAll sends all the elements in the list on the stream.
	SendAll(v []bigquery.ValueSaver)

	// Flush writes all elements currently held in memory to BigQuery.
	Flush()

	// Complete sends the signal that the stream is now complete for this iteration to the receiver.
	Complete()
}

type streamImpl struct {
	typ    string
	schema Schema

	object chan bigquery.ValueSaver
	list   chan []bigquery.ValueSaver
	flush  chan struct{}
	done   chan struct{}
}

func (s *streamImpl) Type() string {
	return s.typ
}

func (s *streamImpl) Send(v bigquery.ValueSaver) {
	s.object <- v
}

func (s *streamImpl) SendAll(v []bigquery.ValueSaver) {
	s.list <- v
}

func (s *streamImpl) Flush() {
	s.flush <- struct{}{}
}

func (s *streamImpl) Complete() {
	s.done <- struct{}{}
}
