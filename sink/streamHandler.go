package sink

import (
	"cloud.google.com/go/bigquery"
	"context"
	"fmt"
	"github.com/3lvia/metrics-go/metrics"
	"github.com/pkg/errors"
	"log"
	"time"
)

const (
	metricsTemplateReceived = `sink_%s_received`
	metricsTemplateFlushed = `sink_%s_flushed`
	metricsErrors = `sink_errors`
)

type writeOrchestration func(ctx context.Context, rows []bigquery.ValueSaver, stream targetStream) (string, error)

type streamHandler struct {
	dataset    string
	operations TableOperations
	metrics    metrics.Metrics
}

func (s *streamHandler) start(ctx context.Context, stream targetStream, errorOutput chan<- error) {
	metricsReceived := fmt.Sprintf(metricsTemplateReceived, stream.Type())
	metricsFlushed := fmt.Sprintf(metricsTemplateFlushed, stream.Type())

	var rows []bigquery.ValueSaver
	for {
		select {
		case obj := <-stream.Stream():
			rows = append(rows, obj)
			s.metrics.IncCounter(metricsReceived, metrics.DayLabels())
		case <-stream.Done():
			log.Print(fmt.Sprintf("iteration done received from %s", stream.Type()))
			o := s.orchestration(stream.Schema().Disposition)
			msg, err := o(ctx, rows, stream)

			if err != nil {
				s.reportErr(err, msg, errorOutput)
			}
			rows = []bigquery.ValueSaver{}
			c := s.metrics.Counter(metricsFlushed, metrics.DayLabels())
			c.Add(float64(len(rows)))
		}
	}
}

func (s *streamHandler) writeTruncate(ctx context.Context, rows []bigquery.ValueSaver, stream targetStream) (string, error) {
	tempTableName := tempTable(stream.Schema().BQSchema.Name, time.Now().UTC())
	tempSchema := tempTableSchema(tempTableName, stream.Schema())
	tempTable, err := s.operations.CreateTable(ctx, s.dataset, tempSchema)
	if err != nil {
		return "while creating temporary table", err
	}

	err = s.operations.Write(ctx, tempTable, rows)
	if err != nil {
		return "while writing to temporary table", err
	}

	table := s.operations.TableRef(s.dataset, stream.Schema())
	err = s.operations.DeleteTable(ctx, table)
	if err != nil {
		return "while deleting existing table", err
	}

	table, err = s.operations.CreateTable(ctx, s.dataset, stream.Schema())
	if err != nil {
		return "while creating table", err
	}
	
	err = s.operations.CopyTable(ctx, tempTable, table)
	if err != nil {
		return "while copying data from temp table", err
	}
	
	err = s.operations.DeleteTable(ctx, tempTable)
	if err != nil {
		return "while deleting temp table", err
	}

	return "", nil
}

func (s *streamHandler) writeAppend(ctx context.Context, rows []bigquery.ValueSaver, stream targetStream) (string, error) {
	table, err := s.operations.CreateTable(ctx, s.dataset, stream.Schema())
	if err != nil {
		return "while creating table", err
	}
	err = s.operations.Write(ctx, table, rows)
	if err != nil {
		return "while writing directly", err
	}
	return "", nil
}

func (s *streamHandler) orchestration(disposition bigquery.TableWriteDisposition) writeOrchestration {
	if disposition == bigquery.WriteAppend {
		return s.writeAppend
	}
	return s.writeTruncate
}

func (s *streamHandler) reportErr(err error, msg string, errorOutput chan<- error) {
	errorOutput <- errors.Wrap(err, msg)
	s.metrics.IncCounter(metricsErrors, metrics.DayLabels())
}