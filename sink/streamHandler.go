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

type writeOrchestration func(ctx context.Context, rows []bigquery.ValueSaver, stream *streamImpl, previouslyFlushed, done bool) (string, error)

type streamHandler struct {
	dataset    string
	operations TableOperations
	tempTable  *bigquery.Table
	metrics    metrics.Metrics
}

func (s *streamHandler) start(ctx context.Context, stream *streamImpl, errorOutput chan<- error) {
	metricsReceived := fmt.Sprintf(metricsTemplateReceived, stream.Type())
	metricsFlushed := fmt.Sprintf(metricsTemplateFlushed, stream.Type())

	o := s.orchestration(stream.schema.Disposition)
	previouslyFlushed := false

	var rows []bigquery.ValueSaver
	for {
		select {
		case obj := <-stream.object:
			rows = append(rows, obj)
			s.metrics.IncCounter(metricsReceived, metrics.DayLabels())
		case objs := <-stream.list:
			for _, obj := range objs {
				rows = append(rows, obj)
				s.metrics.IncCounter(metricsReceived, metrics.DayLabels())
			}
		case <-stream.flush:
			s.flush(ctx, o, rows, stream, previouslyFlushed, false, errorOutput, metricsFlushed)
			rows = []bigquery.ValueSaver{}
			previouslyFlushed = true
		case <-stream.done:
			s.flush(ctx, o, rows, stream, previouslyFlushed, true, errorOutput, metricsFlushed)
			rows = []bigquery.ValueSaver{}
			previouslyFlushed = false
		}
	}
}

func (s *streamHandler) flush(
	ctx context.Context,
	o writeOrchestration,
	rows []bigquery.ValueSaver,
	stream *streamImpl,
	previouslyFlushed bool,
	done bool,
	errorOutput chan<- error,
	metricsFlushed string) {
	op := "flush"
	if done {
		op = "done"
	}
	log.Print(fmt.Sprintf("iteration %s received from %s", op, stream.Type()))

	msg, err := o(ctx, rows, stream, previouslyFlushed, done)
	if err != nil {
		s.reportErr(err, msg, errorOutput)
	}

	c := s.metrics.Counter(metricsFlushed, metrics.DayLabels())
	c.Add(float64(len(rows)))
}

func (s *streamHandler) writeTruncate(ctx context.Context, rows []bigquery.ValueSaver, stream *streamImpl, previouslyFlushed, done bool) (string, error) {
	if !previouslyFlushed {
		tempTableName := tempTable(stream.schema.BQSchema.Name, time.Now().UTC())
		tempSchema := tempTableSchema(tempTableName, stream.schema)
		tt, err := s.operations.CreateTable(ctx, s.dataset, tempSchema)
		s.tempTable = tt
		if err != nil {
			return "while creating temporary table", err
		}
	}
	
	err := s.operations.Write(ctx, s.tempTable, rows)
	if err != nil {
		return "while writing to temporary table", err
	}

	if !done {
		return "", nil
	}
	
	table, err := s.operations.CreateTable(ctx, s.dataset, stream.schema)
	if err != nil {
		return "while creating table", err
	}
	
	err = s.operations.CopyTable(ctx, s.tempTable, table)
	if err != nil {
		return "while copying data from temp table", err
	}
	
	err = s.operations.DeleteTable(ctx, s.tempTable)
	if err != nil {
		return "while deleting temp table", err
	}

	return "", nil
}

func (s *streamHandler) writeAppend(ctx context.Context, rows []bigquery.ValueSaver, stream *streamImpl, previouslyFlushed, done bool) (string, error) {
	var table *bigquery.Table
	var err error

	if previouslyFlushed {
		table = s.operations.TableRef(s.dataset, stream.schema)
	} else {
		table, err = s.operations.CreateTable(ctx, s.dataset, stream.schema)
		if err != nil {
			return "while creating table", err
		}
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