"""Stand-in for the dbt CLI. Writes run_results.json into --target-path, then waits while the
file named by FAKE_DBT_GATE exists, and exits 1 when FAKE_DBT_FAIL is set."""

import os
import shutil
import sys
import time
from pathlib import Path

args = sys.argv[1:]
target = Path(args[args.index("--target-path") + 1])
target.mkdir(parents=True, exist_ok=True)
shutil.copy(Path(__file__).with_name("run_results.json"), target / "run_results.json")
gate = os.environ.get("FAKE_DBT_GATE")
while gate and Path(gate).exists():
    time.sleep(0.02)
print('{"info": {"msg": "fake dbt"}}')
sys.exit(1 if os.environ.get("FAKE_DBT_FAIL") else 0)
