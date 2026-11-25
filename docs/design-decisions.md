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
