# mycli — worked eventkit example

A minimal CLI showing the eventkit integration pattern: in-process capture via the `Collector`, durable disk queue via `FileEmitter`, and a detached `send-metrics` subprocess that ships queued events to Google Analytics 4 via `transport/ga4`.

## Run it

Three demo commands, each emitting an event with a different shape:

```bash
go run ./examples/mycli foo   # Timer + one attribute
go run ./examples/mycli bar   # Timer + two Counters + multiple attributes
go run ./examples/mycli baz   # Timer + Counter; exits non-zero, tags status=error

# inspect what landed on disk
ls ~/.mycli/eventsData/
cat ~/.mycli/eventsData/*.evtq | jq .
```

Each invocation emits one event and spawns a detached `send-metrics` subprocess on exit.

A `.evtq` file is one batch of events. The filename is the first 22 chars of base64-URL-encoded MD5 of the file contents — used by the flusher to detect corruption.

## Ship events to GA4

`send-metrics` is gated on `MYCLI_GA4_ENDPOINT`. Point it at a proxy that injects the GA4 credentials, or at GA4 directly with the credentials set locally:

```bash
# via a proxy that injects measurement_id + api_secret
export MYCLI_GA4_ENDPOINT=https://your-proxy.example.com/collect
go run ./examples/mycli foo

# or talk to GA4 directly
export MYCLI_GA4_ENDPOINT=https://www.google-analytics.com/mp/collect
export MYCLI_GA4_MEASUREMENT_ID=G-XXXXXXXXXX
export MYCLI_GA4_API_SECRET=xxxxxxxxxxxxxxxxxxxxxx
go run ./examples/mycli foo
```

If `MYCLI_GA4_ENDPOINT` is unset, the `send-metrics` subprocess exits cleanly without touching the queue — useful for local development. `MYCLI_GA4_MEASUREMENT_ID` and `MYCLI_GA4_API_SECRET` are optional; either is appended to the request query string when set, and otherwise expected to be injected by the proxy. Events should appear in GA4's **DebugView** within seconds and in standard reports within 24–48 hours.

## Opt-out

| Var | Effect |
|---|---|
| `MYCLI_DISABLE_METRICS=1` | `NullEmitter` is wired in; nothing is written to disk; subprocess is not spawned |
| `MYCLI_DISABLE_EVENT_FLUSH=1` | Disk writes still happen but the `send-metrics` subprocess is not spawned (debugging aid — leaves files for inspection) |

## What to copy into your own CLI

The pattern boils down to four code locations:

1. **Startup** (`runInstrumented`): construct `FileEmitter`, wrap in a `Collector` with `WithDistinctID` / `WithAppName` / `WithAppVersion` / `WithDisabled`, set it as the global.
2. **Per-command instrumentation** (`doFoo` / `doBar` / `doBaz`): `NewEvent` + `defer Global().CloseEventAndAdd(evt)`; enrich with `SetAttribute`, `AddMetric(NewCounter(...))`, `AddMetric(NewTimer(...))`.
3. **Shutdown** (end of `runInstrumented`): bounded `Collector.Close(ctx)` to flush the final partial batch to disk; spawn detached `send-metrics`.
4. **Hidden subcommand** (`runSendMetrics`): build a `ga4.Emitter`, wrap in a `FileFlusher`, call `Flush(ctx)`. The flusher takes the cross-process lock, ships every queued batch (chunked at GA4's 25-events-per-request cap), and deletes only the files whose events all delivered successfully.
