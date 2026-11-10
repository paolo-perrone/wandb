package server

import (
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/wandb/wandb/core/internal/stream"
)

// startServerForShutdownTest runs Serve with one open client connection.
//
// It returns the client end of the connection and Serve's result channel.
func startServerForShutdownTest(
	t *testing.T,
	grace time.Duration,
	configure ...func(*Server),
) (*Server, func(), <-chan error) {
	t.Helper()
	tempRoot := t.TempDir()
	t.Setenv("TMPDIR", tempRoot)

	s := NewServer(ServerParams{Detached: true, ShutdownGracePeriod: grace})
	for _, f := range configure {
		f(s)
	}
	srvCh := make(chan error, 1)
	go func() { srvCh <- s.Serve(filepath.Join(tempRoot, "port.txt")) }()

	client := addTestConnection(t, s)
	return s, func() { _ = client.Close() }, srvCh
}

func requireServeReturnsWithin(t *testing.T, srvCh <-chan error, d time.Duration) {
	t.Helper()
	select {
	case err := <-srvCh:
		require.NoError(t, err)
	case <-time.After(d):
		t.Fatalf("Serve() did not return within %v", d)
	}
}

func requireServeStillRunning(t *testing.T, s *Server, srvCh <-chan error) {
	t.Helper()
	select {
	case err := <-srvCh:
		t.Fatalf("Serve() returned during the grace period: %v", err)
	case <-time.After(200 * time.Millisecond):
	}
	require.NoError(t, s.serverLifetimeCtx.Err(),
		"connections must not be cancelled during the grace period")
}

func TestShutdown_KeepsConnectionsOpenUntilClientsDisconnect(t *testing.T) {
	s, closeClient, srvCh := startServerForShutdownTest(t, time.Minute)

	s.Shutdown()
	requireServeStillRunning(t, s, srvCh)

	closeClient()
	requireServeReturnsWithin(t, srvCh, 5*time.Second)
}

func TestShutdown_ClosesConnectionsAfterHalfTheGracePeriod(t *testing.T) {
	s, _, srvCh := startServerForShutdownTest(t, 600*time.Millisecond)

	start := time.Now()
	s.Shutdown()

	requireServeReturnsWithin(t, srvCh, 5*time.Second)
	elapsed := time.Since(start)
	// Clients get half of the grace period left after the final flush's
	// reserve, which is a fifth of 600ms.
	require.GreaterOrEqual(t, elapsed, 240*time.Millisecond,
		"clients get about half the grace period")
	require.Less(t, elapsed, 600*time.Millisecond,
		"an idle client must not hold the drain for the full grace period")
}

func TestShutdown_ForceStopEndsDrain(t *testing.T) {
	s, _, srvCh := startServerForShutdownTest(t, time.Minute)

	s.Shutdown()
	requireServeStillRunning(t, s, srvCh)

	s.ForceStop()
	requireServeReturnsWithin(t, srvCh, 5*time.Second)
}

func TestShutdown_ZeroGracePeriodStopsImmediately(t *testing.T) {
	s, _, srvCh := startServerForShutdownTest(t, 0)

	s.Shutdown()

	requireServeReturnsWithin(t, srvCh, 5*time.Second)
	require.True(t, s.forceShutdown.Load())
}

func TestShutdown_RepeatedCallsAreSafe(t *testing.T) {
	s, closeClient, srvCh := startServerForShutdownTest(t, time.Minute)

	for range 10 {
		go s.Shutdown()
	}
	closeClient()

	requireServeReturnsWithin(t, srvCh, 5*time.Second)
}

// blockFlushes makes the server's transaction log flushes hang, like
// writes to a stuck filesystem.
func blockFlushes(t *testing.T) func(*Server) {
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	return func(s *Server) {
		s.flushLogs = func([]*stream.Stream) { <-release }
	}
}

func TestShutdown_StuckLogFlushStaysWithinGracePeriod(t *testing.T) {
	s, closeClient, srvCh := startServerForShutdownTest(
		t, 500*time.Millisecond, blockFlushes(t))
	closeClient()

	start := time.Now()
	s.Shutdown()

	requireServeReturnsWithin(t, srvCh, 5*time.Second)
	require.Less(t, time.Since(start), 650*time.Millisecond,
		"flushes must fit in the grace period")
}

func TestShutdown_ForceStopInterruptsStuckLogFlush(t *testing.T) {
	s, closeClient, srvCh := startServerForShutdownTest(
		t, time.Minute, blockFlushes(t))
	closeClient()

	s.Shutdown()
	time.Sleep(100 * time.Millisecond)
	start := time.Now()
	s.ForceStop()

	requireServeReturnsWithin(t, srvCh, 5*time.Second)
	require.Less(t, time.Since(start), 300*time.Millisecond)
}

func TestShutdown_StalledConnectionDoesNotSkipFinishingRuns(t *testing.T) {
	finished := make(chan int32, 1)
	s, _, srvCh := startServerForShutdownTest(t, time.Second, func(s *Server) {
		s.finishStreams = func(_ []*stream.Stream, exitCode int32) { finished <- exitCode }
	})

	// A connection that never finishes closing, such as one stuck in
	// cleanup after it is cancelled.
	stalled := make(chan struct{})
	t.Cleanup(func() { close(stalled) })
	s.connectionsWG.Go(func() { <-stalled })

	s.Shutdown()

	requireServeReturnsWithin(t, srvCh, 5*time.Second)
	select {
	case exitCode := <-finished:
		require.EqualValues(t, drainExitCode, exitCode)
	default:
		t.Fatal("runs were not finished")
	}
}

func TestShutdown_FinalFlushIncludesRunsCreatedDuringDrain(t *testing.T) {
	runB := &stream.Stream{}
	var mu sync.Mutex
	var flushes [][]*stream.Stream
	releaseFinish := make(chan struct{})
	t.Cleanup(func() { close(releaseFinish) })

	s, closeClient, srvCh := startServerForShutdownTest(t, time.Second, func(s *Server) {
		s.flushLogs = func(streams []*stream.Stream) {
			mu.Lock()
			defer mu.Unlock()
			flushes = append(flushes, streams)
			if len(flushes) == 1 {
				// A client creates run B after the drain's first flush.
				require.NoError(t, s.streamMux.AddStream("run-b", runB))
			}
		}
		// Finishing removes run B from the mux, and its uploads stall.
		s.finishStreams = func([]*stream.Stream, int32) { <-releaseFinish }
	})
	closeClient()

	s.Shutdown()
	requireServeReturnsWithin(t, srvCh, 5*time.Second)

	mu.Lock()
	defer mu.Unlock()
	require.Len(t, flushes, 2)
	require.Contains(t, flushes[1], runB, "the final flush must include run B")
}
