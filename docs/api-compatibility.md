# `/v1` compatibility policy

`/v1` is cf-slatedb's public HTTP API. Its contract is
[`openapi/cf-slatedb-v1.json`](openapi/cf-slatedb-v1.json). The OpenAPI test fails whenever the
code and the checked-in specification differ, and every change to the specification is reviewed in
its diff.

## What `/v1` promises

A client written against `/v1` keeps working for as long as `/v1` is served. Within `/v1`, a
release may only:

- add an endpoint;
- add an optional request field (absent or `null` means the earlier behavior);
- add a response field (clients must ignore fields they do not know);
- add an error `code`, or a new value of a `details.reason` (clients must treat an unknown code by
  its HTTP status and `retryable`);
- relax a limit, such as accepting a larger batch.

It never:

- removes or renames an endpoint, a field, or an error code;
- changes a field's type or meaning;
- makes an optional field required;
- tightens a limit;
- changes when a request is retryable;
- weakens what a read sees at a given consistency.

Anything else is a new major version (`/v2`). During a published deprecation window, `/v1` and
`/v2` are served side by side.

## Examples from v1's history

These shipped inside `/v1` because they are additive:

- `manifestId` on transaction begin, get, and scan (D58), where an old client's request keeps the
  earlier behavior;
- `l0SstIds`, `corruptSsts`, and `droppedSsts` on the compaction status (D62);
- the SST drop and checkpoint-list endpoints (D62).

## Stored data

The manifest and the compactor state carry a `formatVersion` (D64). A release that writes a format
older code cannot read raises it, and older code refuses to read or write data in a newer format
instead of rewriting it. A rollback across such a release therefore stops on the newer data, with
an error naming the version to deploy, and never damages it. Data written before format versions
reads as format 1. The rollback procedure is in [`operations.md`](operations.md).
