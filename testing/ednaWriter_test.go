package testing

import (
	"cloud.google.com/go/bigquery"
	"context"
	"fmt"
	"github.com/3lvia/edna-writer-go/sink"
	"github.com/3lvia/metrics-go/metrics"
	"sync"
	"testing"
	"time"
)

const (
	projectID = "edna-dev-19388"
	datasetID = "dp_leveransemotor_raw"
	//vaultPath = "edna/kv/data/gcp-serviceaccounts/system-monitoring"
)

func Test_Start(t *testing.T) {
	ctx := context.Background()

	countChanges := make(chan metrics.CountChange)
	gaugeChanges := make(chan metrics.GaugeChange)
	m := metrics.New(metrics.WithOutputChannels(countChanges, gaugeChanges))

	//v, err := vault.New()
	//if err != nil {
	//	t.Fatal(err)
	//}
	//err = v.SetDefaultGoogleCredentials(vaultPath, "service-account-key")

	wg := &sync.WaitGroup{}
	wg.Add(1)

	go func(cc <-chan metrics.CountChange, gc <-chan metrics.GaugeChange, wg *sync.WaitGroup) {
		count := 0
		for {
			select {
			case c := <-cc:
				fmt.Printf("count %s changed by %f\n", c.Name, c.Increment)
				count++
				if count >= 3 {
					wg.Done()
				}
			case g := <-gc:
				fmt.Printf("gauge %s changed by %f\n", g.Name, g.Value)
			}
		}
	}(countChanges, gaugeChanges, wg)

	sourceStream := sink.Stream("test", schema())

	ops := &mockTableOperations{}

	sink.Start(
		ctx,
		sink.WithBigQuery(projectID, datasetID),
		sink.WithTableOperations(ops),
		sink.WithMetrics(m))

	startProducer(sourceStream)

	wg.Wait()

	if len(ops.rows) != 3 {
		t.Errorf("unexpected number of rows written, got %d", len(ops.rows))
	}

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

type mockTableOperations struct {
	tableCreations []string
	rows           []bigquery.ValueSaver
}

func (m *mockTableOperations) Write(ctx context.Context, client *bigquery.Client, dataset string, schema sink.Schema, rows []bigquery.ValueSaver) error {
	for _, saver := range rows {
		m.rows = append(m.rows, saver)
	}
	return nil
}

func (m *mockTableOperations) CreateTable(ctx context.Context, client *bigquery.Client, dataset string, schema sink.Schema) error {
	m.tableCreations = append(m.tableCreations, fmt.Sprintf("%s.%s", dataset, schema.BQSchema.Name))
	return nil
}