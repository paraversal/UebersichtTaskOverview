"""Start the server as a detached background process when none is running."""

from __future__ import annotations

import logging
import socket
import subprocess
import sys
import tempfile
import time
from pathlib import Path

logger = logging.getLogger(__name__)

# An auto-started server exits after this long with no producers and no jobs.
AUTO_SPAWN_IDLE_TIMEOUT = 600


class ServerUnavailable(ConnectionError):
    """No server is reachable and one could not be started."""


def server_log_path() -> Path:
    if sys.platform == "darwin":
        base = Path.home() / "Library" / "Logs" / "taskoverview-notifier"
    else:
        base = Path(tempfile.gettempdir()) / "taskoverview-notifier"
    base.mkdir(parents=True, exist_ok=True)
    return base / "server.log"


def is_listening(host: str, port: int, timeout: float = 0.3) -> bool:
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


def ensure_server(host: str, port: int, timeout: float = 5.0) -> None:
    """Make sure a server is listening on host:port, starting one if needed."""
    if is_listening(host, port):
        return

    log_path = server_log_path()
    logger.info("No Job Overview server on %s:%d, starting one (log: %s)", host, port, log_path)
    with open(log_path, "ab") as log:
        subprocess.Popen(
            [
                sys.executable,
                "-m",
                "taskoverview_notifier.server",
                "--host",
                host,
                "--port",
                str(port),
                "--idle-timeout",
                str(AUTO_SPAWN_IDLE_TIMEOUT),
            ],
            stdin=subprocess.DEVNULL,
            stdout=log,
            stderr=log,
            start_new_session=True,  # survive this process exiting or crashing
            close_fds=True,
        )

    # If two scripts race to start a server, one fails to bind and exits;
    # both then connect to the winner, so just wait for *a* listener.
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if is_listening(host, port):
            return
        time.sleep(0.05)
    raise ServerUnavailable(f"Started a server but nothing is listening on {host}:{port}; see {log_path}")
