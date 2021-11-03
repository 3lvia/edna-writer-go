package testing

import (
	"cloud.google.com/go/bigquery"
	"context"
	"fmt"
	"github.com/3lvia/edna-writer-go/sink"
	"github.com/3lvia/metrics-go/metrics"
	"strings"
	"sync"
	"testing"
	"time"
)

const (
	projectID = "my-project"
	datasetID = "domain_area_raw"
)

func Test_Start_WriteAppend(t *testing.T) {
	ctx := context.Background()

	countChanges := make(chan metrics.CountChange)
	gaugeChanges := make(chan metrics.GaugeChange)
	m := metrics.New(metrics.WithOutputChannels(countChanges, gaugeChanges))

	wg := &sync.WaitGroup{}
	wg.Add(1)

	go func(cc <-chan metrics.CountChange, gc <-chan metrics.GaugeChange, wg *sync.WaitGroup) {
		count := 0
		for {
			select {
			case c := <-cc:
				fmt.Printf("count %s changed by %f\n", c.Name, c.Increment)
				count++
				if count >= 4 {
					wg.Done()
				}
			case g := <-gc:
				fmt.Printf("gauge %s changed by %f\n", g.Name, g.Value)
			}
		}
	}(countChanges, gaugeChanges, wg)

	sourceStream := sink.Stream("test", schema(bigquery.WriteAppend))

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

	if len(ops.tableCreations) != 1 {
		t.Errorf("unexpected number of table creations, got %d", len(ops.tableCreations))
	}
	tc := ops.tableCreations[0]
	if !strings.Contains(tc, "domain_area_raw.integration_test_truncate") {
		t.Errorf("unexpected temp table created, got %s", tc)
	}

	if len(ops.tableCopyOperations) != 0 {
		t.Errorf("unexpected number of copy operations, got %d", len(ops.tableCopyOperations))
	}

	if len(ops.tableDeletions) != 0 {
		t.Errorf("unexpected number of table deletions, got %d", len(ops.tableDeletions))
	}
}

func Test_Start_WriteTruncate(t *testing.T) {
	ctx := context.Background()

	countChanges := make(chan metrics.CountChange)
	gaugeChanges := make(chan metrics.GaugeChange)
	m := metrics.New(metrics.WithOutputChannels(countChanges, gaugeChanges))

	wg := &sync.WaitGroup{}
	wg.Add(1)

	go func(cc <-chan metrics.CountChange, gc <-chan metrics.GaugeChange, wg *sync.WaitGroup) {
		count := 0
		for {
			select {
			case c := <-cc:
				fmt.Printf("count %s changed by %f\n", c.Name, c.Increment)
				count++
				if count >= 4 {
					wg.Done()
				}
			case g := <-gc:
				fmt.Printf("gauge %s changed by %f\n", g.Name, g.Value)
			}
		}
	}(countChanges, gaugeChanges, wg)

	sourceStream := sink.Stream("test", schema(bigquery.WriteTruncate))

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

	if len(ops.tableCreations) != 2 {
		t.Errorf("unexpected number of table creations, got %d", len(ops.tableCreations))
	}
	tc := ops.tableCreations[0]
	if !strings.Contains(tc, "domain_area_raw.integration_test_truncate") {
		t.Errorf("unexpected temp table created, got %s", tc)
	}
	tc = ops.tableCreations[1]
	if tc != "domain_area_raw.integration_test_truncate" {
		t.Errorf("unexpected table created, got %s", tc)
	}

	if len(ops.tableCopyOperations) != 1 {
		t.Errorf("unexpected number of copy operations, got %d", len(ops.tableCopyOperations))
	}

	if len(ops.tableDeletions) != 2 {
		t.Errorf("unexpected number of table deletions, got %d", len(ops.tableDeletions))
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

func schema(disposition bigquery.TableWriteDisposition) sink.Schema {
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
		Disposition:      disposition,
	}
}

type mockTableOperations struct {
	tableCreations      []string
	tableCopyOperations []string
	tableDeletions      []string
	rows                []bigquery.ValueSaver
}

func (m *mockTableOperations) Write(ctx context.Context, table *bigquery.Table, rows []bigquery.ValueSaver) error {
	for _, saver := range rows {
		m.rows = append(m.rows, saver)
	}
	return nil
}

func (m *mockTableOperations) CreateTable(ctx context.Context, dataset string, schema sink.Schema) (*bigquery.Table, error) {
	m.tableCreations = append(m.tableCreations, fmt.Sprintf("%s.%s", dataset, schema.BQSchema.Name))

	return m.TableRef(dataset, schema), nil
}

func (m *mockTableOperations) CopyTable(ctx context.Context, source, dest *bigquery.Table) error {
	op := fmt.Sprintf("%s.%s -> %s.%s", source.DatasetID, source.TableID, dest.DatasetID, dest.TableID)
	m.tableCopyOperations = append(m.tableCopyOperations, op)
	return nil
}

func (m *mockTableOperations) DeleteTable(ctx context.Context, table *bigquery.Table) error {
	m.tableDeletions = append(m.tableDeletions, fmt.Sprintf("%s.%s", table.DatasetID, table.TableID))
	return nil
}

func (m *mockTableOperations) TableRef(dataset string, schema sink.Schema) *bigquery.Table {
	return &bigquery.Table{DatasetID: dataset, TableID: schema.BQSchema.Name}
}