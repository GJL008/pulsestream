"""Build first. Starts PulseStream in isolation and tests its TCP API, then cleans up."""
import argparse
import socket
import subprocess
import tempfile
import time
from pathlib import Path
from client import Client, ServerError, encode, read

ROOT = Path(__file__).resolve().parent.parent


def free_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def start(java, port, data=None):
    cmd = [java, "-jar", str(ROOT / "target" / "pulsestream-1.0.0.jar"), str(port)]
    if data is not None:
        cmd.append(str(data))
    process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                               creationflags=subprocess.CREATE_NO_WINDOW if hasattr(subprocess, "CREATE_NO_WINDOW") else 0)
    for _ in range(150):
        if process.poll() is not None:
            raise RuntimeError(process.stdout.read().decode(errors="replace"))
        try:
            with Client(port) as client:
                assert client.command("PING") == b"PONG"
            return process
        except OSError:
            time.sleep(.05)
    process.kill()
    process.wait()
    raise TimeoutError("server startup timed out")


def stop(process):
    process.kill()
    process.wait(timeout=10)
    process.stdout.close()


def check(java):
    port = free_port()
    with tempfile.TemporaryDirectory(prefix="pulsestream-smoke-") as data:
        pulse = start(java, port, data)
        try:
            with Client(port) as client:
                for i in range(20):
                    assert client.command("PRODUCE", "events", bytes([i, 0, 255])) == i
                batch = client.command("CONSUME", "events", 0, 16)
                assert batch[0] == 16
                assert batch[1][0][2] == bytes([0, 0, 255])
                assert client.command("CONSUME", "events", 16)[0] == 20
        finally:
            stop(pulse)  # Abrupt process termination after acknowledged writes.
        pulse = start(java, port, data)
        try:
            with Client(port) as client:
                assert client.command("CONSUME", "events", 19)[1][0][2] == bytes([19, 0, 255])
                assert client.command("PRODUCE", "events", "after-restart") == 20
            print("PASS PulseStream: binary messages, batch cursor, acknowledged data after process kill, continued offsets")
        finally:
            stop(pulse)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--java", default="java")
    check(parser.parse_args().java)
