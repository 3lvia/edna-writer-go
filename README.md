# edna-writer

This package provides a way for clients to write data to BigQuery through the usage of a streaming metaphor.

## Details
It is the responsibility of the client code to provide the schema and other specifics of the BigQuery dataset and table, and then this package handles the actual writing of data.

### Writing and completing
In order to write elements on to the stream, the **Send** function is called. Also, if you have a list of elements to send, the function **SendAll** may be used.

Currently, real streaming is not implemented, and this package rather caches rows in memory before flushing to BigQuery when the calling code sends a signal.
It is the client's responsibility to "complete" the streaming iteration by calling the **Complete** function on the stream.

### Intermediate flush
Memory consumption may become an issue of there are many elements to write. To counter this, the stream has a function **Flush** that
can be used to write the elements currently in memory to BigQuery.

## Configuration
This module assumes that a service account key file for a service account having write access to BigQuery already is set as follows:

```
import "os"

keyFile := `absolute path to credentials file`
os.Setenv("GOOGLE_APPLICATION_CREDENTIALS", keyFile)
```

This module must be configured with the Google project ID and dataset ID to write to.
These values are set with the options pattern:

```
import "github.com/3lvia/edna-writer-go/sink"

googleProjectID := "my-google-project"
datasetID := "my-dataset"
sink.Start(
   ... Other parameters ...
   m.WithBigQuery(googleProjectID, datasetID))
```

## Metrics
This module is integrated with the module **github.com/3lvia/metrics-go**, which again publishes metrics interally as Prometheuse metrics.

The metrics service is set via the option pattern:

```
import (
  "github.com/3lvia/edna-writer-go/sink"
  "github.com/3lvia/metrics-go/metrics"
)

m := metrics.New()
sink.Start(
   ... Other parameters ...
   m.WithMetrics(m))
```
The number of elements received on the stream and the number of times the stream is flushed to BigQuery are captured in counter metrics.
Since there may be many streams registered, the type of the stream is part of the metric name so that each stream can be traced separately.

The metrics names follow the pattern **sink_[type]_[operation]** . In the examples below a single stream of type **initiative** has been registered.

```
# HELP sink_initiative_flushed
# TYPE sink_initiative_flushed counter
sink_initiative_flushed{day="2021-11-04"} 1
# HELP sink_initiative_received
# TYPE sink_initiative_received counter
sink_initiative_received{day="2021-11-04"} 72
```
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