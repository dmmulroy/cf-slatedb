# Design decisions

Each entry records a place where the implementation needed a decision the design page did not make,
or deviates from it, with the reason. Newest last.

## D1. Repository layout and package names

- Libraries: `@cf-slatedb/lsm-core` (`packages/lsm-core`), `@cf-slatedb/lsm-cloudflare`
  (`packages/lsm-cloudflare`), and `@cf-slatedb/sharding` (`packages/lsm-sharding`, named as in
  design §7 "the optional `@cf-slatedb/sharding` package").
- Stack: `@cf-slatedb/infra` (`infra/cf-slatedb`), no `exports`.
- Tools: `@cf-slatedb/tools-*`.

Reason: the brief's layout (design §10) with iac's naming conventions. iac's `shared-infra`
distinction does not apply here: the libraries are the product, and there is one Stack.

## D2. Workspace boundary rules

iac's boundary test is adapted rather than copied: in addition to "no `exports` under `infra/`" and
"no relative import leaves its workspace", it enforces the two rules that are specific to this
design: `@cf-slatedb/lsm-core` imports no Cloudflare or Alchemy module, and
`@cf-slatedb/sharding` uses only the public entrypoints of the two libraries (design §12: "The
sharding package only calls public APIs").

## D3. Formatter ignores the design page

`docs/show-me-r2-lsm-design.html` is the authored source of truth and is excluded from `vp fmt`, so
the formatter never rewrites it.

## D4. Workerd test harness packages

`@alchemy.run/test@2.0.0-beta.87` and `@effect/vitest@4.1.4` are added to the catalog as
test-only dependencies. In beta.87 Alchemy's test harness (`Test.make({ dev: true })`) moved out
of `alchemy` into `@alchemy.run/test`; cf-twitch pins the same versions. They are part of the same
Alchemy and Effect releases, run only locally, and do not change the architecture, so they do not
need the "Stop and ask" approval for new dependencies.

## D5. Workaround: flush OTLP telemetry before an event settles (alchemy 2.0.0-beta.87)

alchemy 2.0.0-beta.87 still exports an event's telemetry only when the request scope closes inside
`waitUntil` after the event settles (`processEvent` in `alchemy/src/Workers/WorkerBridge.ts`,
`execute` in `DurableObjectBridge.ts`), the bug reproduced by `../alchemy-workerd-otlp-flush-repro`.
Alchemy is **not patched**. Instead every handler is wrapped with `withEventTelemetryFlush`
(`packages/lsm-cloudflare/src/lsm-telemetry.ts`), which runs the handler and then, in
`Effect.ensuring`, awaits the event's `OtlpExporter.Flusher.flush` (bounded at 10 seconds). The
bridge builds that `Flusher` into every event's context, so the handler can reach it.

What it covers: every span that ends inside the handler (`Effect.fn` service spans, the handler's
own span, outbound HTTP client spans to Durable Objects and `BlockServer`) is exported before the
event settles. What it cannot cover: the HTTP server span the bridge's tracer middleware opens around
the handler ends after the handler returns, so it still goes out through the bridge's own
`waitUntil` flush. While the runtime keeps running (normal `alchemy dev` and production), it arrives;
if the runtime is torn down immediately after the event, it can be lost, and its children then show
as roots in the trace CLI. Alarm handlers and Workflow runs have no bridge-owned span, so they are
fully covered.

Rule: apply `withEventTelemetryFlush` outermost on every `fetch`, `alarm`, Workflow run, and cron
handler.

Evidence: `packages/lsm-cloudflare/test/telemetry/telemetry-smoke.workerd.ts` tears workerd down
right after the event with a collector that answers after 500 ms. With the wrapper, the Durable
Object's span and the Worker's client span both arrive, in one trace; with the wrapper made a no-op,
the test fails. A second test checks full parentage, server spans included, while workerd keeps
running; a third checks alarm spans.

Trade-off: a response waits for its span export. Locally that is a loopback POST to the trace CLI.
Remove the wrapper when an Alchemy release flushes before settling.

## D6. Local state store for the Stack

`infra/cf-slatedb/alchemy.run.ts` uses `localState()` rather than iac's `Cloudflare.state()`,
because no live stage exists and `Cloudflare.state()` needs account credentials. Switching to
`Cloudflare.state()` is part of preparing a live stage, which needs approval.

## D7. Span names of HTTP spans

Effect 4.1.4 names HTTP client and server spans by method (`GET`, `POST`) and distinguishes them by
span kind (server 2, client 3). The trace CLI shows the kind next to the name, and trace assertions
on HTTP spans match on name plus parent rather than on an `http.server` prefix.
