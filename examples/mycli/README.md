# mycli — worked eventkit example

A minimal CLI showing the eventkit integration pattern: in-process capture via the `Collector`, durable disk queue via `FileEmitter`, and a detached `send-metrics` subprocess that ships queued events to PostHog via `transport/posthog`.

## Run it

```bash
# emits one instrumented event; spawns a detached send-metrics subprocess on exit
go run ./examples/mycli demo

# inspect what landed on disk
ls ~/.mycli/eventsData/
cat ~/.mycli/eventsData/*.evtq | jq .
```

A `.evtq` file is one batch of events. The filename is the first 22 chars of base64-URL-encoded MD5 of the file contents — used by the flusher to detect corruption.

## Ship events to PostHog

```bash
export MYCLI_POSTHOG_API_KEY=phc_xxx
go run ./examples/mycli demo
# the spawned send-metrics subprocess will deliver to PostHog and delete the file
```

If `MYCLI_POSTHOG_API_KEY` is unset, the `send-metrics` subprocess exits cleanly without touching the queue — useful for local development.

## Opt-out

| Var | Effect |
|---|---|
| `MYCLI_DISABLE_METRICS=1` | `NullEmitter` is wired in; nothing is written to disk; subprocess is not spawned |
| `MYCLI_DISABLE_EVENT_FLUSH=1` | Disk writes still happen but the `send-metrics` subprocess is not spawned (debugging aid — leaves files for inspection) |

## What to copy into your own CLI

The pattern boils down to four code locations:

1. **Startup** (`runDemo`): construct `FileEmitter`, wrap in a `Collector` with `WithDistinctID` / `WithAppName` / `WithAppVersion` / `WithDisabled`, set it as the global.
2. **Per-command instrumentation** (`doCloneLikeWork`): `NewEvent` + `defer Global().CloseEventAndAdd(evt)`; enrich with `SetAttribute`, `AddMetric(NewCounter(...))`, `AddMetric(NewTimer(...))`.
3. **Shutdown** (end of `runDemo`): bounded `Collector.Close(ctx)` to flush the final partial batch to disk; spawn detached `send-metrics`.
4. **Hidden subcommand** (`runSendMetrics`): build a `PostHog.Emitter`, wrap in a `FileFlusher`, call `Flush(ctx)`. The flusher takes the cross-process lock, ships every queued batch via the SDK's batching, and deletes only the files whose events all delivered successfully.
