package sink

import (
	"cloud.google.com/go/bigquery"
	"context"
	"fmt"
	"github.com/3lvia/edna-writer/types"
	"strings"
	"time"
)

func write(ctx context.Context, client *bigquery.Client, dataset string, schema types.Schema, rows []bigquery.ValueSaver) error {
	err := createTable(ctx, client, dataset, schema)
	if err != nil {
		return err
	}

	// Write directly to the table and be finished if the disposition is WriteAppend
	finalTable := client.Dataset(dataset).Table(schema.BQSchema.Name)
	if schema.Disposition == bigquery.WriteAppend {
		return writeDirect(ctx, finalTable, rows)
	}

	// The disposition != WriteAppend and we are assuming WriteTruncate. This operation has 4 steps:
	// STEP 1: Write the data to a temporary table
	tempTable := client.Dataset(dataset).Table(tempTable(schema.BQSchema.Name, time.Now().UTC()))
	err = writeDirect(ctx, tempTable, rows)
	if err != nil {
		return err
	}

	// STEP 2: Delete the final table (with data from previous runs)
	err = finalTable.Delete(ctx)
	if err != nil {
		return err
	}

	// STEP 3: Copy the data from the temporary to the final table.
	copier := finalTable.CopierFrom(tempTable)
	j, err := copier.Run(ctx)
	if err != nil {
		return err
	}
	status, err := j.Wait(ctx)
	if err != nil {
		return err
	}
	if status.Err() != nil {
		return status.Err()
	}

	// STEP 4: Delete the temporary table
	return tempTable.Delete(ctx)

}

func writeDirect(ctx context.Context, table *bigquery.Table, rows []bigquery.ValueSaver) error {
	inserter := table.Inserter()
	if err := inserter.Put(ctx, rows); err != nil {
		return err
	}
	return nil
}

func createTable(ctx context.Context, client *bigquery.Client, dataset string, schema types.Schema) error {

	tableRef := client.Dataset(dataset).Table(schema.BQSchema.Name)
	if err := tableRef.Create(ctx, schema.BQSchema); err != nil {
		if !strings.Contains(err.Error(), "googleapi: Error 409: Already Exists:") {
			return err
		}
	}
	return nil
}

func tempTable(base string, d time.Time) string {
	return fmt.Sprintf("%s_%s", base, d.Format("200601021504"))
}