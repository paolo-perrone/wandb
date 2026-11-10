package server

import (
	"log/slog"
	"time"

	"github.com/wandb/wandb/core/internal/stream"
)

// drainExitCode is the exit code for runs whose client disconnected without
// finishing them during a shutdown.
const drainExitCode = 255

// maxLogWriteTime bounds each transaction log flush during a drain, so that
// a stuck filesystem cannot hold up the rest of the drain.
const maxLogWriteTime = time.Second

// drain gives active runs up to the shutdown grace period to finish.
//
// The listeners must already be closed. Records already written to each
// run's transaction log buffer are flushed to the file first.
//
// For the first half of the remaining time, existing connections stay open:
// a client that is finishing its run (for example, from its own SIGTERM
// handler) can complete it, and a client that sends a teardown request shuts
// the server down as usual. Connections still open after that are closed.
// Once every connection has closed, runs that are still open are finished
// with drainExitCode.
//
// The end of the grace period is reserved for a final flush, so that records
// that arrived during the drain also reach the file.
//
// It returns within the grace period, and soon after ForceStop.
func (s *Server) drain() {
	grace := s.shutdownGracePeriod
	end := time.Now().Add(grace)
	finalFlushStart := end.Add(-min(maxLogWriteTime, grace/5))
	clientDeadline := time.Now().Add(time.Until(finalFlushStart) / 2)

	// Capture the streams now: finishing a stream removes it from the mux.
	// The final flush also covers runs started during the drain.
	streams := s.streamMux.Streams()
	defer func() {
		s.writeLogsUntil(
			s.flushLogs,
			append(streams, s.streamMux.Streams()...),
			end,
		)
	}()
	s.writeLogsUntil(s.flushLogs, streams, finalFlushStart)

	if !s.waitForClients(clientDeadline, finalFlushStart) {
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
	case <-time.After(time.Until(finalFlushStart)):
		slog.Warn("server: shutdown grace period expired finishing runs")
	case <-s.forceCh:
		slog.Warn("server: shutdown forced while finishing runs")
	}
}

// waitForClients waits for every connection to close.
//
// Connections still open at clientDeadline are closed. It returns false if
// they have not all closed by giveUp, or on ForceStop.
func (s *Server) waitForClients(clientDeadline, giveUp time.Time) bool {
	connectionsDone := s.waitForConnectionsToFinish()

	select {
	case <-connectionsDone:
		return true
	case <-time.After(time.Until(clientDeadline)):
		slog.Warn("server: clients did not disconnect, closing connections")
		s.stopServer()
	case <-s.forceCh:
		slog.Warn("server: shutdown forced while waiting for clients")
		return false
	}

	select {
	case <-connectionsDone:
		return true
	case <-time.After(time.Until(giveUp)):
		slog.Warn("server: shutdown grace period expired closing connections")
		return false
	case <-s.forceCh:
		slog.Warn("server: shutdown forced while closing connections")
		return false
	}
}

// writeLogsUntil runs write on the streams' transaction logs and waits for
// it for at most maxLogWriteTime, until the deadline, or until ForceStop.
//
// A write that is still running when this returns may finish before the
// process exits.
func (s *Server) writeLogsUntil(
	write func([]*stream.Stream),
	streams []*stream.Stream,
	deadline time.Time,
) {
	done := make(chan struct{})
	go func() {
		defer close(done)
		write(streams)
	}()

	timeout := min(maxLogWriteTime, time.Until(deadline))
	select {
	case <-done:
	case <-time.After(timeout):
		slog.Warn("server: stopped waiting for transaction logs to be written")
	case <-s.forceCh:
		slog.Warn("server: shutdown forced while writing transaction logs")
	}
}
