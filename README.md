# eventkit

Fire-and-forget event telemetry for Go CLIs. Buffers events in-process, spills to a durable disk queue, and ships them out-of-band via a detached flusher subprocess so the user-facing command never blocks on the network.

```
Event → Collector → FileEmitter → .evtq on disk → FileFlusher → transport (GA4)
```

## Install

```bash
go get github.com/dolthub/eventkit
```

Requires Go 1.26+.

## Usage

See [`examples/mycli`](examples/mycli) for the worked pattern: construct a `FileEmitter`, wrap it in a `Collector`, emit events with `NewEvent` + `AddMetric` + `CloseEventAndAdd`, then on exit spawn a detached subcommand that runs `FileFlusher.Flush` against `transport/ga4`.

## Durability

Each batch is a single `.evtq` file named by the first 22 chars of base64-URL MD5 of its contents (corruption check). A cross-process `eventkit.lock` ensures only one flusher runs at a time. If `Send` returns an error, the file stays on disk for the next run; on success it is deleted.

## Transports

- `transport/ga4` — ships via the GA4 Measurement Protocol over plain HTTP.
- Roll your own by implementing `Emitter`.
