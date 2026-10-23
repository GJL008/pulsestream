"""Repeated isolated PulseStream benchmark. Requires a built JAR."""
import argparse
import json
import platform
import subprocess
import sys
import tempfile
from pathlib import Path
from smoke import ROOT, free_port, start, stop


def measure(args):
    args.output.mkdir(parents=True, exist_ok=True)
    metadata = {
        "java": subprocess.check_output([args.java, "-version"], stderr=subprocess.STDOUT, text=True),
        "platform": platform.platform(), "processor": platform.processor(),
        "warning": "Short local loopback runs, not a saturation test. Each run starts a fresh JVM.",
    }
    (args.output / "environment.json").write_text(json.dumps(metadata, indent=2) + "\n", encoding="utf-8")
    for run in range(1, args.runs + 1):
        with tempfile.TemporaryDirectory(prefix="pulsestream-measure-") as data:
            port = free_port()
            server = start(args.java, port, data)
            try:
                subprocess.run([sys.executable, str(ROOT / "tools/benchmark.py"),
                                "--port", str(port), "--requests", str(args.requests),
                                "--connections", "16", "--payload", "256", "--warmup", "100",
                                "--output", str(args.output / f"run-{run}.json")], check=True)
            finally:
                stop(server)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--java", default="java")
    parser.add_argument("--runs", type=int, default=3)
    parser.add_argument("--requests", type=int, default=5000)
    parser.add_argument("--output", type=Path, default=ROOT / "benchmarks/latest")
    measure(parser.parse_args())
