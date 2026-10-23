# PulseStream: engineering decisions

## Independent build

All source and transport code lives in this repository. Maven produces one executable JAR with no external runtime dependencies. JUnit is a test-only dependency. There is no parent POM or dependency on another portfolio repository.

## Transport

TCP uses bounded RESP2 arrays of binary bulk strings. Each admitted connection has a virtual thread, with at most 256 clients. Request payloads and argument counts are limited. Storage errors preserve a valid session; malformed framing ends it. Shutdown closes sockets before waiting for workers. See [protocol](PROTOCOL.md).

## Commit and recovery

Append writes a complete framed record and calls force(true) before publishing its offset. ReentrantLock serializes readers and writers of one topic; topics can progress independently. File I/O is not performed while holding an intrinsic monitor.

An IOException during append marks the topic unavailable until recovery. Startup checks segment bases, record bounds and CRC32C. Only an incomplete final tail is truncated; corruption in complete records fails closed. Reads also verify CRC.

## Delivery boundaries

A lost acknowledgement can leave a durable record without the producer knowing. Retrying can duplicate messages. There is no exactly-once delivery or producer idempotency. Consumers own their durable cursors. Process-kill recovery tests do not prove survival of storage power failure; parent-directory syncing is not implemented.

## Next experiments

Measure group commit against force-per-record with explicit queue bounds and acknowledgement budgets. Investigate sparse indexing for larger logs, a durable manifest for segment inventory, and platform-specific directory syncing. Replication, zero-copy, consumer groups and retention are not implemented.

## Evidence

Unit tests cover engine and transport invariants; the smoke script exercises the packaged JAR in a separate process. The load generator is a closed-loop, same-machine end-to-end measurement, not maximum engine throughput. See [benchmarks](../benchmarks/README.md) for scope and raw reports.
