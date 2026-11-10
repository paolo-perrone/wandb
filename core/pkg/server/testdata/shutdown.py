"""Logs to an offline run and waits to be stopped by shutdown_process_test.go.

With SHUTDOWN_APP_FINISHES=1, SIGTERM makes the script log one more row and
finish the run with exit code 3, like an application that handles SIGTERM.
"""

import json
import os
import pathlib
import signal
import sys
import time

import psutil

import wandb
import wandb.sdk.lib.service.service_process as service_process

service_process.get_core_path = lambda: os.environ["WANDB_TEST_CORE_BINARY"]

run = wandb.init(
    mode="offline",
    project="shutdown-test",
    dir=os.environ["SHUTDOWN_DIR"],
    settings=wandb.Settings(
        # Keep records in the writer's buffer until the shutdown flushes them.
        x_transaction_log_flush_interval=3600,
        x_disable_stats=True,
        console="off",
    ),
)
for i in range(25):
    run.log({"row": i})

if os.environ.get("SHUTDOWN_APP_FINISHES") == "1":

    def finish_on_sigterm(signum, frame):
        run.log({"row": 25})
        run.finish(exit_code=3)
        sys.exit(0)

    signal.signal(signal.SIGTERM, finish_on_sigterm)

# A round trip after logging ensures wandb-core has received the records.
run._interface.deliver_get_summary().wait_or(timeout=10)

core = next(
    p for p in psutil.Process().children(recursive=True) if "wandb-core" in p.name()
)
pathlib.Path(os.environ["SHUTDOWN_DIR"], "ready.json").write_text(
    json.dumps({"pid": core.pid, "file": run.settings.sync_file})
)
while True:
    time.sleep(1)
