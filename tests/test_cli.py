import subprocess
import sys

from event_model import __version__


def test_cli_version():
    cmd = [sys.executable, "-m", "event_model", "--version"]
    assert subprocess.check_output(cmd).decode().strip() == __version__
