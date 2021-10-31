# edna-writer

This package provides a way for clients to write data to BigQuery through the usage of a streaming metaphor.

It is the responsibility of the client code to provide the schema and other specifics of the BigQuery dataset and table, and then this package handles the actual writing of data.

Currently, real streaming is not implemented, and this package rather caches rows in memory before flushing to BigQuery when the calling code sends a signal.

## Configuration

## Metrics

## Usage

```
import (
    "cloud.google.com/go/bigquery"
    "context"
    "github.com/3lvia/edna-writer-go/sink"
    "github.com/3lvia/metrics-go/metrics"
)

func main() {
    ctx := context.Background()
    m := metrics.New()

    var streams []sink.TargetStream

    mySourceStream, myTargetStream := sink.Streams("my-data", schema())
    sourceStream := sink.Stream("test", schema()))
    
    startProducer(mySourceStream)
    
	sink.Start(
		ctx,
		sink.WithBigQuery("google-project-id", "dataset-id"),
		sink.WithTableOperations(ops),
		sink.WithMetrics(m))
}

func startProducer(ss sink.SourceStream) {
	go func() {
		for i := 0; i < 3; i++ {
			r := &row{
				s: fmt.Sprintf("%d", i),
				i: i,
				t: time.Now().UTC(),
			}
			ss.Send(r)
		}
		ss.Complete()
	}()
}

type row struct {
	s string
	i int
	t time.Time
}

func (r *row) Save() (row map[string]bigquery.Value, insertID string, err error) {
	m := map[string]bigquery.Value {
		"stringColumn": r.s,
		"intColumn": r.i,
		"timeColumn": r.t,
	}
	return m, "", nil
}

func schema() sink.Schema {
	var s bigquery.Schema
	s = append(s, &bigquery.FieldSchema{
		Name: "stringColumn",
		Description: "",
		Required: false,
		Type: bigquery.StringFieldType,
	})
	s = append(s, &bigquery.FieldSchema{
		Name: "intColumn",
		Description: "",
		Required: false,
		Type: bigquery.IntegerFieldType,
	})
	s = append(s, &bigquery.FieldSchema{
		Name: "timeColumn",
		Description: "",
		Required: false,
		Type: bigquery.TimeFieldType,
	})
	return sink.Schema{
		BQSchema:         &bigquery.TableMetadata{
			Name:        "integration_test_truncate",
			Description: "Table for testing",
			Schema:      s,
		},
		Disposition:      bigquery.WriteTruncate,
	}
}
```