# PulseStream wire protocol

The transport accepts the RESP2 subset consisting of arrays of non-null bulk strings. Every argument is raw bytes. Command names and numeric arguments are interpreted as UTF-8 text by the service. There is no inline-command parser or nested request support.

Example `SET key hello`:

```text
*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nhello\r\n
```

Lengths count bytes, not Unicode characters. CRLF inside a bulk payload is ordinary data. Requests can be pipelined on one connection; responses are returned in request order.

| Limit | Value |
| --- | ---: |
| Arguments per command | 1..16 |
| Total bulk payload bytes per command | 1,048,576 |
| Single bulk argument | 1,048,576 |
| Admitted connections per server | 256 |
| Socket read inactivity timeout | 30 seconds |

PulseStream imposes smaller domain limits on messages and batches. The read timeout is an inactivity timeout, not an absolute request deadline; TCP writes have no explicit timeout. This service is intended for trusted local clients.

Responses use RESP bulk strings, signed integers, null bulk (`$-1\r\n`), arrays, or `-ERR message\r\n`. Even `OK` and `PONG` are bulk strings. Application errors preserve the connection. Malformed request framing returns an error and closes the connection. Excess connections are closed without a protocol response.

PulseStream supports `METRICS`, which returns transport counters: active connections, commands read, request/protocol errors, and rejected connection admissions. The METRICS request itself counts as a command. Counter values are approximate concurrent snapshots and reset at startup.

The Python client is a small reference implementation. It is not a general Redis client. Do not assume compatibility with Redis authentication, subscriptions, RESP3, redirects, or client handshake extensions.
