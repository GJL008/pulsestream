"""Dependency-free RESP client. Also imported by smoke tests and benchmarks."""
import argparse
import json
import socket


def encode(*args):
    parts = [f"*{len(args)}\r\n".encode()]
    for arg in args:
        value = arg if isinstance(arg, bytes) else str(arg).encode()
        parts.extend((f"${len(value)}\r\n".encode(), value, b"\r\n"))
    return b"".join(parts)


class ServerError(Exception):
    pass


def read(stream):
    line = stream.readline()
    if not line.endswith(b"\r\n"):
        raise ConnectionError("truncated response")
    kind, data = line[:1], line[1:-2]
    if kind == b"-":
        raise ServerError(data.decode(errors="replace"))
    if kind == b":":
        return int(data)
    if kind == b"$":
        size = int(data)
        if size == -1:
            return None
        value = stream.read(size)
        if len(value) != size or stream.read(2) != b"\r\n":
            raise ConnectionError("truncated bulk response")
        return value
    if kind == b"*":
        return [read(stream) for _ in range(int(data))]
    raise ConnectionError("unsupported response type")


class Client:
    def __init__(self, port, host="127.0.0.1"):
        self.socket = socket.create_connection((host, port), timeout=5)
        self.stream = self.socket.makefile("rb")

    def command(self, *args):
        self.socket.sendall(encode(*args))
        return read(self.stream)

    def close(self):
        self.stream.close()
        self.socket.close()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()


def printable(value):
    if isinstance(value, bytes):
        return value.decode(errors="backslashreplace")
    if isinstance(value, list):
        return [printable(item) for item in value]
    return value


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="PulseStream command client")
    parser.add_argument("--port", type=int, default=9092)
    parser.add_argument("command", nargs="+")
    args = parser.parse_args()
    try:
        with Client(args.port) as client:
            print(json.dumps(printable(client.command(*args.command)), ensure_ascii=False, indent=2))
    except (OSError, ServerError) as error:
        parser.exit(1, f"Error: {error}\n")
