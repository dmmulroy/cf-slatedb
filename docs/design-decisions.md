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

## D5. Workaround: stage-selected OTLP flush policy (alchemy 2.0.0-beta.87)

alchemy 2.0.0-beta.87 still exports an event's telemetry only when the request scope closes inside
`waitUntil` after the event settles (`processEvent` in `alchemy/src/Workers/WorkerBridge.ts`,
`execute` in `DurableObjectBridge.ts`), the bug reproduced by `../alchemy-workerd-otlp-flush-repro`.
Alchemy is **not patched** (Dillon's review). The workaround lives in
`packages/lsm-cloudflare/src/lsm-telemetry.ts` and is a **stage-selected policy**:

- `EventTelemetryFlushPolicy` is `BeforeResponse` or `AfterResponse`.
- `eventTelemetryFlushPolicyForStage(stage)` selects `BeforeResponse` for local stages (`dev`,
  `test`, `local`, and `dev-*`, `test-*`, `local-*`) and `AfterResponse` for every other stage
  (`production`, previews, anything else), so a live stage never adds export latency to responses.
- Every Worker and Durable Object yields `eventTelemetryFlushForStage` in its init Effect (Alchemy
  provides `Stage` at plan time and at runtime) and wraps every `fetch`, `alarm`, Workflow run, and
  cron handler with the returned wrapper, outermost, after `Effect.withSpan`.
- `BeforeResponse`: the wrapper runs the handler, then in `Effect.ensuring` awaits the event's
  `OtlpExporter.Flusher.flush` (bounded at 10 seconds). The bridge builds that `Flusher` into every
  event's context, so the handler can reach it. Every span that ends inside the handler (service
  spans, the handler's own span, outbound HTTP client spans) is exported before the event settles.
  The bridge's HTTP server span ends after the handler, so it still goes out through the bridge's
  `waitUntil` flush; it is lost only if the runtime is torn down immediately, and its children then
  show as roots in the trace CLI. Alarm handlers and Workflow runs have no bridge-owned span.
- `AfterResponse`: the wrapper returns the handler unchanged; the bridge's `waitUntil` flush
  exports everything after the response, as Alchemy intends for live Workers.

Evidence:

- `src/lsm-telemetry.test.ts`: with a recording `Flusher`, `BeforeResponse` flushes after the
  handler and before the result is returned (also on failure); `AfterResponse` never flushes; the
  stage rule and the `Stage`-driven selection.
- `test/telemetry/telemetry-smoke.workerd.ts` (stage `test-telemetry-smoke`, `BeforeResponse`):
  with workerd torn down right after the event and a collector that answers after 500 ms, the
  Durable Object span and the Worker's client span arrive in one trace, and the response waited at
  least 500 ms. Run under a live-style stage name, the same test fails, which shows the policy is
  selected from `Stage` at runtime. Full parentage and alarm spans are checked too.
- `test/telemetry/telemetry-smoke-live-policy.workerd.ts` (stage `preview-telemetry-smoke`,
  `AfterResponse`): with a collector that answers after 2 s, the response returns in under 2 s, and
  the bridge still delivers every span, server spans included, with intact parentage.

Remove the workaround when an Alchemy release flushes before settling.

## D6. Local state store for the Stack

`infra/cf-slatedb/alchemy.run.ts` uses `localState()` rather than iac's `Cloudflare.state()`,
because no live stage exists and `Cloudflare.state()` needs account credentials. Switching to
`Cloudflare.state()` is part of preparing a live stage, which needs approval.

## D7. Span names of HTTP spans

Effect 4.1.4 names HTTP client and server spans by method (`GET`, `POST`) and distinguishes them by
span kind (server 2, client 3). The trace CLI shows the kind next to the name, and trace assertions
on HTTP spans match on name plus parent rather than on an `http.server` prefix.
