package sink

import (
	"cloud.google.com/go/bigquery"
	"context"
	"fmt"
	"strings"
	"time"
)

type TableOperations interface {
	Write(ctx context.Context, table *bigquery.Table, rows []bigquery.ValueSaver) error
	CreateTable(ctx context.Context, dataset string, schema Schema) (*bigquery.Table, error)
	CopyTable(ctx context.Context, source, dest *bigquery.Table) error
	DeleteTable(ctx context.Context, table *bigquery.Table) error
	TableRef(dataset string, schema Schema) *bigquery.Table
}

type tableOperations struct {
	client *bigquery.Client
}

func (o *tableOperations) Write(ctx context.Context,  table *bigquery.Table, rows []bigquery.ValueSaver) error {
	inserter := table.Inserter()
	if err := inserter.Put(ctx, rows); err != nil {
		return err
	}
	return nil
}

func (o *tableOperations) CreateTable(ctx context.Context, dataset string, schema Schema) (*bigquery.Table, error) {
	client := o.client
	tableRef := client.Dataset(dataset).Table(schema.BQSchema.Name)
	if err := tableRef.Create(ctx, schema.BQSchema); err != nil {
		if !strings.Contains(err.Error(), "googleapi: Error 409: Already Exists:") {
			return nil, err
		}
	}
	return tableRef, nil
}

func (o *tableOperations) CopyTable(ctx context.Context, source, dest *bigquery.Table) error {
	return nil
}

func (o *tableOperations) DeleteTable(ctx context.Context, table *bigquery.Table) error {
	return nil
}

func (o *tableOperations) TableRef(dataset string, schema Schema) *bigquery.Table {
	return o.client.Dataset(dataset).Table(schema.BQSchema.Name)
}

func tempTable(base string, d time.Time) string {
	return fmt.Sprintf("%s_%s", base, d.Format("200601021504"))
}

func tempTableSchema(table string, s Schema) Schema {
	return Schema{
		BQSchema:    &bigquery.TableMetadata{
			Name: table,
			Schema: s.BQSchema.Schema,
		},
		Disposition: bigquery.WriteEmpty,
	}
}