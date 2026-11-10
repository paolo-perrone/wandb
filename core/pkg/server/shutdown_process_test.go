//go:build !windows

package server

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/wandb/wandb/core/internal/observability"
	"github.com/wandb/wandb/core/internal/transactionlog"
)

// shutdownScenario is one way of stopping wandb-core in
// TestShutdownProcess_PreservesOfflineRun.
type shutdownScenario struct {
	name string

	// appFinishes makes testdata/shutdown.py finish its run on SIGTERM.
	appFinishes bool

	// exitThenWait makes testdata/shutdown.py send its exit record and
	// then wait without finishing the run.
	exitThenWait bool

	// stop stops the processes.
	stop func(t *testing.T, worker *os.Process, corePID int)

	// wantRows and wantExitCode are the expected transaction log contents.
	// A nil wantExitCode means no exit record.
	wantRows     int
	wantExitCode *int32
}

func exitCode(code int32) *int32 { return &code }

// This runs the Python SDK against a built wandb-core.
// Set WANDB_TEST_PYTHON and WANDB_TEST_CORE_BINARY to opt in.
func TestShutdownProcess_PreservesOfflineRun(t *testing.T) {
	python := os.Getenv("WANDB_TEST_PYTHON")
	if python == "" || os.Getenv("WANDB_TEST_CORE_BINARY") == "" {
		t.Skip("requires a built wandb-core and Python with wandb's dependencies and psutil")
	}
	sdkRoot, err := filepath.Abs("../../..")
	require.NoError(t, err)

	scenarios := []shutdownScenario{
		{
			name: "sigterm to core",
			stop: func(t *testing.T, _ *os.Process, corePID int) {
				require.NoError(t, syscall.Kill(corePID, syscall.SIGTERM))
			},
			wantRows:     25,
			wantExitCode: exitCode(drainExitCode),
		},
		{
			name: "parent killed",
			stop: func(t *testing.T, worker *os.Process, _ int) {
				require.NoError(t, worker.Kill())
			},
			wantRows:     25,
			wantExitCode: exitCode(drainExitCode),
		},
		{
			// A scheduler signals every process in the job at once, and the
			// application finishes its run from its SIGTERM handler.
			name:        "sigterm to both, app finishes",
			appFinishes: true,
			stop: func(t *testing.T, worker *os.Process, corePID int) {
				require.NoError(t, syscall.Kill(corePID, syscall.SIGTERM))
				require.NoError(t, worker.Signal(syscall.SIGTERM))
			},
			wantRows:     26,
			wantExitCode: exitCode(3),
		},
		{
			// The app is killed after its run's exit record but before it
			// tells wandb-core the run is finished. The drain must not add a
			// second exit record, which would make the log fail to sync.
			name:         "parent killed after exit",
			exitThenWait: true,
			stop: func(t *testing.T, worker *os.Process, _ int) {
				require.NoError(t, worker.Kill())
			},
			wantRows:     25,
			wantExitCode: exitCode(3),
		},
		{
			// Control: SIGKILL runs no shutdown code, so buffered rows are lost.
			name: "sigkill control",
			stop: func(t *testing.T, _ *os.Process, corePID int) {
				require.NoError(t, syscall.Kill(corePID, syscall.SIGKILL))
			},
			wantRows: -1, // fewer than 25
		},
	}

	for _, sc := range scenarios {
		t.Run(sc.name, func(t *testing.T) {
			logFile := runShutdownScenario(t, python, sdkRoot, sc)
			rows, code, exits := readHistoryAndExit(t, logFile)

			if sc.wantRows < 0 {
				require.Less(t, rows, 25)
			} else {
				require.Equal(t, sc.wantRows, rows)
			}
			if sc.wantExitCode == nil {
				require.Nil(t, code)
			} else {
				require.NotNil(t, code, "no exit record")
				require.Equal(t, *sc.wantExitCode, *code)
				require.Equal(t, 1, exits, "exactly one exit record")
			}
		})
	}
}

// shutdownReady is what testdata/shutdown.py writes once it has logged.
type shutdownReady struct {
	PID  int    `json:"pid"`
	File string `json:"file"`
}

// runShutdownScenario runs testdata/shutdown.py, stops it as the scenario
// says, waits for wandb-core to exit and returns the transaction log path.
func runShutdownScenario(
	t *testing.T,
	python, sdkRoot string,
	sc shutdownScenario,
) string {
	t.Helper()
	root := t.TempDir()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	t.Cleanup(cancel)

	worker := exec.CommandContext(ctx, python, "testdata/shutdown.py")
	worker.Env = append(os.Environ(),
		"PYTHONPATH="+sdkRoot,
		"SHUTDOWN_DIR="+root,
		"WANDB_SILENT=true",
	)
	if sc.appFinishes {
		worker.Env = append(worker.Env, "SHUTDOWN_APP_FINISHES=1")
	}
	if sc.exitThenWait {
		worker.Env = append(worker.Env, "SHUTDOWN_EXIT_THEN_WAIT=1")
	}
	log, err := os.Create(filepath.Join(root, "worker.log"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = log.Close() })
	worker.Stdout, worker.Stderr = log, log
	require.NoError(t, worker.Start())
	t.Cleanup(func() { _ = worker.Process.Kill(); _ = worker.Wait() })

	var ready shutdownReady
	require.Eventually(t, func() bool {
		data, err := os.ReadFile(filepath.Join(root, "ready.json"))
		return err == nil && json.Unmarshal(data, &ready) == nil
	}, 30*time.Second, 20*time.Millisecond, "worker did not become ready")
	t.Cleanup(func() { _ = syscall.Kill(ready.PID, syscall.SIGKILL) })

	sc.stop(t, worker.Process, ready.PID)
	waitForExit(t, ctx, python, ready.PID)
	return ready.File
}

// waitForExit waits until the process exits or becomes a zombie.
func waitForExit(t *testing.T, ctx context.Context, python string, pid int) {
	t.Helper()
	wait := exec.CommandContext(ctx, python, "-c", `import psutil, sys, time
pid = int(sys.argv[1])
while psutil.pid_exists(pid):
    try:
        if psutil.Process(pid).status() == psutil.STATUS_ZOMBIE:
            break
    except psutil.NoSuchProcess:
        break
    time.sleep(0.02)`, strconv.Itoa(pid))
	require.NoError(t, wait.Run())
}

// readHistoryAndExit counts history records and exit records, and returns
// the last exit code.
//
// A partially written last block ends the read.
func readHistoryAndExit(t *testing.T, logFile string) (int, *int32, int) {
	t.Helper()
	reader, err := transactionlog.OpenReader(logFile, observability.NewNoOpLogger())
	require.NoError(t, err)
	defer reader.Close()

	var rows, exits int
	var code *int32
	for {
		record, err := reader.Read()
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return rows, code, exits
		}
		require.NoError(t, err)
		if record.GetHistory() != nil {
			rows++
		}
		if exit := record.GetExit(); exit != nil {
			value := exit.ExitCode
			code = &value
			exits++
		}
	}
}
