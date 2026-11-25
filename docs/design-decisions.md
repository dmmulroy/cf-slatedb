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

## D5. Patch: flush OTLP telemetry before an event settles (alchemy 2.0.0-beta.87)

`patches/alchemy@2.0.0-beta.87.patch` (applied by pnpm `patchedDependencies`) ports the fix from
`../alchemy-workerd-otlp-flush-repro` to beta.87's bridges: `processEvent` in
`lib/Workers/WorkerBridge.js` and `execute` in `lib/Workers/DurableObjectBridge.js` capture the
event's telemetry Context and await `OtlpExporter.Flusher.flush` (bounded at 10 seconds) before
the event's promise settles, then close the request scope into `waitUntil` as before.

Evidence: `packages/lsm-cloudflare/test/telemetry/telemetry-smoke.workerd.ts` tears workerd down
right after the event with a collector that answers after 500 ms. Without the patch, 0 of 2
exports complete (red); with it, both do, with intact Worker → Durable Object parentage (green).
Alarm spans arrive in both cases while workerd stays up.

Trade-off: a request's response now waits for its span export. Locally that is a loopback POST to
the trace CLI. For a live stage, export to a nearby collector or rely on Workers Observability.
Remove the patch when an Alchemy release flushes before settling.

## D6. Local state store for the Stack

`infra/cf-slatedb/alchemy.run.ts` uses `localState()` rather than iac's `Cloudflare.state()`,
because no live stage exists and `Cloudflare.state()` needs account credentials. Switching to
`Cloudflare.state()` is part of preparing a live stage, which needs approval.

## D7. Span names of HTTP spans

Effect 4.1.4 names HTTP client and server spans by method (`GET`, `POST`) and distinguishes them by
span kind (server 2, client 3). The trace CLI shows the kind next to the name, and trace assertions
on HTTP spans match on name plus parent rather than on an `http.server` prefix.
