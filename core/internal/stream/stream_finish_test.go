package stream

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/wandb/wandb/core/internal/observabilitytest"
	"github.com/wandb/wandb/core/internal/runwork"
	"github.com/wandb/wandb/core/internal/settings"
	spb "github.com/wandb/wandb/core/pkg/service_go_proto"
)

// delayedExitWork schedules like RunExitWork: the record is only queued
// after other work finishes (for RunExitWork, the TensorBoard handler).
type delayedExitWork struct {
	runwork.WorkImpl
	release <-chan struct{}
}

func (w *delayedExitWork) Schedule(wg *sync.WaitGroup, proceed func()) {
	wg.Add(1)
	go func() {
		defer wg.Done()
		<-w.release
		proceed()
	}()
}

type delayedExitParser struct{ release <-chan struct{} }

func (p delayedExitParser) Parse(record *spb.Record) runwork.WorkImpl {
	work := runwork.WorkFromRecord(record)
	if record.GetExit() != nil {
		return &delayedExitWork{WorkImpl: work, release: p.release}
	}
	return work
}

func TestFinishAndClose_WaitsForClientExitToBeQueued(t *testing.T) {
	logger := observabilitytest.NewTestLogger(t)
	release := make(chan struct{})
	s := &Stream{
		logger:       logger,
		runWork:      runwork.New(BufferSize, logger),
		recordParser: delayedExitParser{release: release},
		settings:     settings.From(&spb.Settings{Silent: wrapperspb.Bool(true)}),
	}

	var exitCodes []int32
	consumed := make(chan struct{})
	go func() {
		defer close(consumed)
		for work := range s.runWork.Chan() {
			if exit := work.ToRecord().GetExit(); exit != nil {
				exitCodes = append(exitCodes, exit.ExitCode)
			}
		}
	}()

	// The client sends its exit record, which waits to be queued, and
	// disconnects. Then the server finishes the stream.
	s.HandleRecord(&spb.Record{
		RecordType: &spb.Record_Exit{Exit: &spb.RunExitRecord{ExitCode: 3}},
	}, nil)
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		s.FinishAndClose(255)
	}()

	select {
	case <-finished:
		t.Fatal("FinishAndClose returned before the client's exit was queued")
	case <-time.After(200 * time.Millisecond):
	}

	close(release)
	select {
	case <-finished:
	case <-time.After(5 * time.Second):
		t.Fatal("FinishAndClose did not return")
	}
	<-consumed
	require.Equal(t, []int32{3}, exitCodes, "exactly the client's exit")
}

// slowClientExitParser pauses parsing the client's exit record (code 3),
// so that a shutdown can start in the middle of handling it.
type slowClientExitParser struct {
	parsing chan<- struct{}
	release <-chan struct{}
}

func (p slowClientExitParser) Parse(record *spb.Record) runwork.WorkImpl {
	if record.GetExit().GetExitCode() == 3 {
		close(p.parsing)
		<-p.release
	}
	return runwork.WorkFromRecord(record)
}

// startExitConsumer records the exit codes that reach RunWork and answers
// exit requests, as the Sender does once a run finishes.
func startExitConsumer(s *Stream) (exitCodes *[]int32, done <-chan struct{}) {
	codes := &[]int32{}
	consumed := make(chan struct{})
	go func() {
		defer close(consumed)
		for work := range s.runWork.Chan() {
			if exit := work.ToRecord().GetExit(); exit != nil {
				*codes = append(*codes, exit.ExitCode)
				work.Request.Respond(&spb.ServerResponse{})
			}
		}
	}()
	return codes, consumed
}

func newStreamForFinishTest(t *testing.T, parser RecordParser) *Stream {
	logger := observabilitytest.NewTestLogger(t)
	return &Stream{
		logger:       logger,
		runWork:      runwork.New(BufferSize, logger),
		recordParser: parser,
		settings:     settings.From(&spb.Settings{Silent: wrapperspb.Bool(true)}),
	}
}

func TestFinishAndClose_ClientExitBeingParsedWins(t *testing.T) {
	parsing := make(chan struct{})
	release := make(chan struct{})
	s := newStreamForFinishTest(t,
		slowClientExitParser{parsing: parsing, release: release})
	exitCodes, consumed := startExitConsumer(s)

	go s.HandleRecord(&spb.Record{
		RecordType: &spb.Record_Exit{Exit: &spb.RunExitRecord{ExitCode: 3}},
	}, nil)
	<-parsing // the client's exit is being parsed

	finished := make(chan struct{})
	go func() {
		defer close(finished)
		s.FinishAndClose(255)
	}()
	time.Sleep(100 * time.Millisecond)
	close(release)

	select {
	case <-finished:
	case <-time.After(5 * time.Second):
		t.Fatal("FinishAndClose did not return")
	}
	<-consumed
	require.Equal(t, []int32{3}, *exitCodes, "exactly the client's exit")
}

func TestFinishAndClose_LateClientExitIsNotSavedTwice(t *testing.T) {
	s := newStreamForFinishTest(t, delayedExitParser{release: closedChan()})
	exitCodes, consumed := startExitConsumer(s)

	s.FinishAndClose(255)
	// A client exit that arrives after the stream finished is dropped.
	s.HandleRecord(&spb.Record{
		RecordType: &spb.Record_Exit{Exit: &spb.RunExitRecord{ExitCode: 3}},
	}, nil)

	<-consumed
	require.Equal(t, []int32{255}, *exitCodes)
}

func closedChan() <-chan struct{} {
	ch := make(chan struct{})
	close(ch)
	return ch
}
