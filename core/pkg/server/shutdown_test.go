package server

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// startServerForShutdownTest runs Serve with one open client connection.
//
// It returns the client end of the connection and Serve's result channel.
func startServerForShutdownTest(
	t *testing.T,
	grace time.Duration,
) (*Server, func(), <-chan error) {
	t.Helper()
	tempRoot := t.TempDir()
	t.Setenv("TMPDIR", tempRoot)

	s := NewServer(ServerParams{Detached: true, ShutdownGracePeriod: grace})
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
	require.GreaterOrEqual(t, elapsed, 300*time.Millisecond,
		"clients get half the grace period")
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
