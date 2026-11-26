import { Effect, Layer } from "effect";
import { HttpApiBuilder } from "effect/http-api";

import { DbWriter } from "./db-writer.ts";
import { lsmHttpServerLayer } from "./lsm-http-server.ts";
import { WriterHttpApi } from "./writer-http-api.ts";

/** Handlers of the writer HTTP API, delegating each parsed request to the contextual `DbWriter`. */
export const writerHttpHandlersLayer = HttpApiBuilder.group(WriterHttpApi, "writer", (handlers) =>
  Effect.gen(function* () {
    const writer = yield* DbWriter;
    return handlers
      .handle("write", ({ params, payload }) => writer.write(params.databaseId, payload))
      .handle("commitTransaction", ({ params, payload }) =>
        writer.commitTransaction(params.databaseId, payload),
      )
      .handle("probe", ({ params, payload }) => writer.probe(params.databaseId, payload.key))
      .handle("scan", ({ params, payload }) => writer.scan(params.databaseId, payload))
      .handle("flush", ({ params }) =>
        writer.flush(params.databaseId).pipe(Effect.map((flushedSeq) => ({ flushedSeq }))),
      )
      .handle("changes", ({ params, query }) =>
        writer.changes(params.databaseId, query.afterSeq, query.limit),
      )
      .handle("status", ({ params }) => writer.status(params.databaseId))
      .handle("destroy", ({ params }) => writer.destroy(params.databaseId))
      .handle("seal", ({ params, payload }) =>
        writer.seal(params.databaseId, payload.successors, payload.holdForSuccessors),
      );
  }),
);

/** Every writer HTTP route, needing only a `DbWriter`. */
export const writerHttpRouterLayer = HttpApiBuilder.layer(WriterHttpApi).pipe(
  Layer.provide(writerHttpHandlersLayer),
  Layer.provide(lsmHttpServerLayer),
);
