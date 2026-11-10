package stream

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/wandb/wandb/core/internal/observability"
	"github.com/wandb/wandb/core/internal/transactionlog"
	spb "github.com/wandb/wandb/core/pkg/service_go_proto"
)

func TestFlushTransactionLogs_WritesBufferedRecords(t *testing.T) {
	path := filepath.Join(t.TempDir(), "run.wandb")
	w, err := transactionlog.OpenWriter(path)
	require.NoError(t, err)
	defer func() { _ = w.Close() }()
	require.NoError(t, w.Write(&spb.Record{Num: 42}))

	logger := observability.NewNoOpLogger()
	r, err := transactionlog.OpenReader(path, logger)
	require.NoError(t, err)
	defer r.Close()

	// Control: the record is still only in the writer's buffer.
	_, err = r.Read()
	require.Error(t, err)
	require.NoError(t, r.ResetLastRead())

	stream := &Stream{logger: logger}
	stream.transactionWriter.Store((&WriterFactory{Logger: logger}).New(w))
	mux := NewStreamMux()
	require.NoError(t, mux.AddStream("run", stream))

	FlushTransactionLogs(mux.Streams())

	record, err := r.Read()
	require.NoError(t, err)
	require.EqualValues(t, 42, record.Num)
}

func TestFlushTransactionLogs_SkipsStreamsWithoutLog(t *testing.T) {
	FlushTransactionLogs([]*Stream{{logger: observability.NewNoOpLogger()}})
}
