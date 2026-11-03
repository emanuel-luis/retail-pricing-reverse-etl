import shutil
import subprocess
from pathlib import Path

import pytest

from retail_pricing.report import parse_steps

ROOT = Path(__file__).resolve().parents[2]
SCRIPTS = ["demo.sh", "heavy.sh", "lib.sh", "sample-stats.sh"]
BASH = shutil.which("bash")
pytestmark = pytest.mark.skipif(BASH is None, reason="bash not available")


def bash(args: list[str], cwd: Path = ROOT) -> subprocess.CompletedProcess[str]:
    assert BASH is not None
    return subprocess.run(
        [BASH, *args], check=False, cwd=cwd, capture_output=True, text=True, timeout=60
    )


@pytest.mark.parametrize("name", SCRIPTS)
def test_script_syntax(name: str) -> None:
    result = bash(["-n", f"scripts/{name}"])
    assert result.returncode == 0, result.stderr


@pytest.mark.parametrize("name", SCRIPTS)
def test_script_is_lf(name: str) -> None:
    assert b"\r" not in (ROOT / "scripts" / name).read_bytes()


def test_step_times_records_and_stops_on_failure(tmp_path: Path) -> None:
    script = (
        ". scripts/lib.sh; RUN_DIR=out; mkdir -p out; ok() { true; }; bad() { false; echo no; };"
        " step 'first step' ok; step 'second step' bad; echo unreachable"
    )
    result = bash(
        [
            "-c",
            f"set -euo pipefail; cd {tmp_path.as_posix()}; {script.replace('scripts/', ROOT.as_posix() + '/scripts/')}",
        ]
    )
    assert result.returncode == 1, result.stderr
    assert "unreachable" not in result.stdout
    steps = parse_steps((tmp_path / "out" / "steps.json").read_text(encoding="utf-8"))
    assert [(s["step"], s["ok"]) for s in steps] == [("first step", True), ("second step", False)]
