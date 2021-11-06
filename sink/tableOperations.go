package sink

import (
	"cloud.google.com/go/bigquery"
	"context"
	"fmt"
	"strings"
	"time"
)

// TableOperations provides an abstraction for the concrete operations that happen against BigQuery.
type TableOperations interface {
	// Write writes the given value to the table in BigQuery
	Write(ctx context.Context, table *bigquery.Table, rows []bigquery.ValueSaver) error

	// CreateTable creates the table in BigQuery.
	CreateTable(ctx context.Context, dataset string, schema Schema) (*bigquery.Table, error)

	// CopyTable copies the content of the source table to the destination table.
	CopyTable(ctx context.Context, source, dest *bigquery.Table) error

	// DeleteTable deletes the table in BigQuery.
	DeleteTable(ctx context.Context, table *bigquery.Table) error

	// TableRef creates and returns a reference to the table with the given schema and schema. The name of the table
	// is embedded in the schema.
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
	copier := dest.CopierFrom(source)
	copier.WriteDisposition = bigquery.WriteTruncate
	j, err := copier.Run(ctx)
	if err != nil {
		return err
	}
	status, err := j.Wait(ctx)
	if err != nil {
		return err
	}
	if err := status.Err(); err != nil {
		return err
	}
	return nil
}

func (o *tableOperations) DeleteTable(ctx context.Context, table *bigquery.Table) error {
	return table.Delete(ctx)
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