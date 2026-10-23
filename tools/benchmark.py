"""Closed-loop end-to-end benchmark; each connection has one outstanding request."""
import argparse
import asyncio
import datetime
import json
import math
import platform
import time
from pathlib import Path
from client import encode


async def response(reader):
    line = await reader.readuntil(b"\r\n")
    kind, value = line[:1], line[1:-2]
    if kind == b"-":
        raise RuntimeError(value.decode(errors="replace"))
    if kind == b":":
        return int(value)
    if kind == b"$":
        size = int(value)
        if size == -1:
            return None
        body = await reader.readexactly(size + 2)
        if body[-2:] != b"\r\n":
            raise RuntimeError("invalid response terminator")
        return body[:-2]
    if kind == b"*":
        return [await response(reader) for _ in range(int(value))]
    raise RuntimeError("invalid response")


async def run(args):
    payload = b"x" * args.payload
    topic = "bench_" + str(time.time_ns())
    ready = asyncio.Event()
    connected = 0

    async def worker(worker_id):
        nonlocal connected
        reader, writer = await asyncio.wait_for(asyncio.open_connection("127.0.0.1", args.port), 5)
        samples = []

        async def request(frame):
            writer.write(frame)
            await writer.drain()
            return await asyncio.wait_for(response(reader), 10)

        try:
            # Warm up transport without adding unmeasured records to the topic.
            for _ in range(args.warmup):
                await request(encode("PING"))
            connected += 1
            if connected == args.connections:
                ready.set()
            await asyncio.wait_for(ready.wait(), 30)
            start = time.perf_counter()
            for i in range(worker_id, args.requests, args.connections):
                frame = encode("PRODUCE", topic, payload)
                before = time.perf_counter_ns()
                result = await request(frame)
                samples.append((time.perf_counter_ns() - before) / 1_000_000)
                if not isinstance(result, int):
                    raise RuntimeError("incorrect broker response")
            return start, time.perf_counter(), samples
        finally:
            writer.close()
            await writer.wait_closed()

    jobs = [asyncio.create_task(worker(i)) for i in range(args.connections)]
    try:
        results = await asyncio.gather(*jobs)
    except BaseException:
        for job in jobs:
            job.cancel()
        await asyncio.gather(*jobs, return_exceptions=True)
        raise
    elapsed = max(item[1] for item in results) - min(item[0] for item in results)
    latencies = sorted(value for item in results for value in item[2])
    percentile = lambda p: round(latencies[max(0, math.ceil(len(latencies) * p) - 1)], 3)
    report = {
        "timestamp_utc": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "mode": args.mode, "requests": len(latencies), "connections": args.connections,
        "payload_bytes": args.payload, "warmup_per_connection": args.warmup,
        "elapsed_seconds": round(elapsed, 4), "requests_per_second": round(len(latencies) / elapsed, 1),
        "latency_ms": {"p50": percentile(.5), "p95": percentile(.95), "p99": percentile(.99), "max": round(max(latencies), 3)},
        "errors": 0, "client_platform": platform.platform(), "python": platform.python_version(),
        "method": "loopback, closed-loop, one outstanding request per connection; client and server share host",
    }
    report["topic"] = topic
    report["durability"] = "FileChannel.force(true) before each acknowledgement"
    return report


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.set_defaults(mode="pulse")
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument("--requests", type=int, default=2000)
    parser.add_argument("--connections", type=int, default=16)
    parser.add_argument("--payload", type=int, default=256)
    parser.add_argument("--warmup", type=int, default=20)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    if not (1 <= args.connections <= 128 and args.connections <= args.requests <= 90_000 and 0 <= args.payload <= 4096 and 1 <= args.warmup <= 1000):
        parser.error("require 1..128 connections <= requests <= 90000, payload 0..4096, warmup 1..1000")
    result = asyncio.run(run(args))
    text = json.dumps(result, indent=2)
    print(text)
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(text + "\n", encoding="utf-8")
