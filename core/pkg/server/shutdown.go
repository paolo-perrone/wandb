package server

import (
	"log/slog"
	"time"

	"github.com/wandb/wandb/core/internal/stream"
)

// drainExitCode is the exit code for runs whose client disconnected without
// finishing them during a shutdown.
const drainExitCode = 255

// drain gives active runs up to the shutdown grace period to finish.
//
// The listeners must already be closed. For the first half of the grace
// period, existing connections stay open: a client that is finishing its run
// (for example, from its own SIGTERM handler) can complete it, and a client
// that sends a teardown request shuts the server down as usual. Connections
// still open after that are closed. Once every connection has closed, runs
// that are still open are finished with drainExitCode.
//
// Records already written to each run's transaction log buffer are flushed
// to the file before waiting and again at the end, so they survive the
// process exiting even if the grace period runs out.
//
// It returns at the end of the grace period, or early on ForceStop.
func (s *Server) drain() {
	deadline := time.NewTimer(s.shutdownGracePeriod)
	defer deadline.Stop()

	// Capture the streams now: finishing a stream removes it from the mux.
	// The final flush also covers runs started during the drain.
	streams := s.streamMux.Streams()
	defer func() {
		flushWithin(append(streams, s.streamMux.Streams()...), flushTimeout)
	}()
	flushWithin(streams, flushTimeout)

	// Clients get the first half of the grace period to finish their runs
	// and disconnect. After that, close their connections so that the rest
	// of the grace period can finish the runs they left open.
	clientDeadline := time.NewTimer(s.shutdownGracePeriod / 2)
	defer clientDeadline.Stop()

	connectionsDone := s.waitForConnectionsToFinish()
	select {
	case <-connectionsDone:
	case <-clientDeadline.C:
		slog.Warn("server: clients did not disconnect, closing connections")
		s.stopServer()

		select {
		case <-connectionsDone:
		case <-deadline.C:
			slog.Warn("server: shutdown grace period expired closing connections")
			return
		case <-s.forceCh:
			slog.Warn("server: shutdown forced while closing connections")
			return
		}
	case <-s.forceCh:
		slog.Warn("server: shutdown forced while waiting for clients")
		return
	}

	finished := make(chan struct{})
	go func() {
		defer close(finished)
		s.streamMux.FinishAndCloseAllStreams(drainExitCode)
	}()

	select {
	case <-finished:
		slog.Info("server: shutdown drain completed")
	case <-deadline.C:
		slog.Warn("server: shutdown grace period expired finishing runs")
	case <-s.forceCh:
		slog.Warn("server: shutdown forced while finishing runs")
	}
}

// flushTimeout bounds each transaction log flush during a drain, so that a
// stuck filesystem cannot keep the process alive past its grace period.
const flushTimeout = time.Second

// flushWithin flushes the streams' transaction logs, giving up after timeout.
func flushWithin(streams []*stream.Stream, timeout time.Duration) {
	done := make(chan struct{})
	go func() {
		defer close(done)
		stream.FlushTransactionLogs(streams)
	}()

	select {
	case <-done:
	case <-time.After(timeout):
		slog.Warn("server: timed out flushing transaction logs")
	}
}
