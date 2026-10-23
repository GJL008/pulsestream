# PulseStream local baseline

Measured 23 October 2026 on Windows 11, OpenJDK 23.0.2 and Python 3.12.14. Sources target Java 21. Raw data and environment details are in [2026-10-23/](2026-10-23/).

| Run | Throughput/s | p50 ms | p95 ms | p99 ms | Max ms |
| --- | ---: | ---: | ---: | ---: | ---: |
| PulseStream PRODUCE, force per record | 5,000 | 2,020.9 records/s | 16.299–20.322 ms | 0 |
| PulseStream 1 | 2,020.9 | 7.179 | 11.474 | 19.477 | 30.296 |
| PulseStream 2 | 2,103.6 | 7.173 | 9.497 | 16.299 | 30.988 |
| PulseStream 3 | 1,899.0 | 7.579 | 12.252 | 20.322 | 44.364 |

Three fresh-JVM loopback runs: 16 connections, one outstanding request per connection, 256-byte payloads, 100 warmup commands per connection and 5000 measured requests per run. All measured responses completed without errors. All measured records go to one topic, with force(true) before each acknowledgement. Warmup uses PING and does not warm up disk appends.

These baseline measurements were recorded before splitting the original source layout into this standalone repository; they are historical performance evidence, not a new benchmark of the packaging change.

Reproduce from this repository:

```powershell
./dev.ps1 build
./dev.ps1 measure
```

Or use `python tools/measure.py --java java --output benchmarks/latest` after building. The driver starts only PulseStream and uses temporary data. To test a running server:

```sh
python tools/benchmark.py --port 9092 --requests 5000 --connections 16 --payload 256 --warmup 100
```

Client and server shared a machine. Python scheduling, short warmup, OS caches and background load influence the result. This is not a saturation test, power-loss test or comparison with other systems. Longer runs, hardware/storage details, allocation/GC profiles and separate client hosts are needed for broader claims.
