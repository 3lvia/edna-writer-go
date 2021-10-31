package testing

import (
	"cloud.google.com/go/bigquery"
	"context"
	"fmt"
	"github.com/3lvia/edna-writer/sink"
	"github.com/3lvia/edna-writer/types"
	"github.com/3lvia/hn-config-lib-go/vault"
	"github.com/3lvia/metrics-go/metrics"
	"sync"
	"testing"
	"time"
)

const (
	projectID = "edna-dev-19388"
	datasetID = "dp_leveransemotor_raw"
	vaultPath = "edna/kv/data/gcp-serviceaccounts/system-monitoring"
)

func Test_Start(t *testing.T) {
	// Arrange
	ctx := context.Background()

	countChanges := make(chan metrics.CountChange)
	gaugeChanges := make(chan metrics.GaugeChange)
	m := metrics.New(metrics.WithOutputChannels(countChanges, gaugeChanges))

	v, err := vault.New()
	if err != nil {
		t.Fatal(err)
	}
	err = v.SetDefaultGoogleCredentials(vaultPath, "service-account-key")

	wg := &sync.WaitGroup{}
	wg.Add(1)

	go func(cc <-chan metrics.CountChange, gc <-chan metrics.GaugeChange) {
		for {
			select {
			case c := <-cc:
				fmt.Printf("count %s changed by %f\n", c.Name, c.Increment)
			case g := <-gc:
				fmt.Printf("gauge %s changed by %f\n", g.Name, g.Value)
			}
		}
	}(countChanges, gaugeChanges)

	sourceStream, targetStream := types.Streams("test", schema())

	sink.Start(
		ctx,
		[]types.TargetStream{targetStream},
		sink.WithBigQuery(projectID, datasetID),
		sink.WithMetrics(m))

	startProducer(sourceStream)

	wg.Wait()

}

func startProducer(ss types.SourceStream) {
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

func schema() types.Schema {
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
	return types.Schema{
		BQSchema:         &bigquery.TableMetadata{
			Name:        "integration_test_truncate",
			Description: "Table for testing",
			Schema:      s,
		},
		Disposition:      bigquery.WriteTruncate,
	}
}
