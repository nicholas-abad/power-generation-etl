"""The Docker application layout must start without any database access."""

from pathlib import Path
import shutil
import subprocess
import sys


def test_docker_runtime_files_support_default_command(tmp_path):
    root = Path(__file__).resolve().parents[1]
    # Materialize only the local COPY instructions of the final Docker stage.
    for line in (root / "Dockerfile").read_text().splitlines():
        if not line.startswith("COPY ") or "--from=" in line:
            continue
        _, source, *_, destination = line.split()
        if not source.endswith("/"):
            continue
        shutil.copytree(root / source, tmp_path / destination)
    result = subprocess.run(
        [sys.executable, "src/database_management.py", "--help"],
        cwd=tmp_path,
        capture_output=True,
        text=True,
        env={"PYTHON_DOTENV_DISABLED": "1"},
    )
    assert result.returncode == 0, result.stderr
    assert "load-data" in result.stdout
