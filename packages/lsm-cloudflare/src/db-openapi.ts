import {
  CheckpointId,
  DatabaseId,
  ManifestId,
  RowSchema,
  SeqNum,
  SortedRunId,
  SstId,
  Timestamp,
  TransactionCommit,
  WriteBatch,
  WriteReceipt,
  WriterEpoch,
  Checkpoint,
} from "@shaledb/lsm-core";
import { Option, Schema } from "effect";
import { OpenApi } from "effect/http-api";

import { SstDropReport } from "./compactor.ts";
import { LSM_ACCESS_TOKEN_CREDENTIAL, unauthenticatedMessage } from "./lsm-access-gate.ts";
import { PinnedSnapshot } from "./database-lifecycle.ts";
import { DbSnapshot } from "./db-client.ts";
import {
  ChangeFeedResponse,
  CheckpointsResponse,
  CompactionStatusResponse,
  CreateCheckpointRequest,
  CreateDatabaseRequest,
  CreateDatabaseResponse,
  DbHttpApi,
  DropSstRequest,
  ForkDatabaseRequest,
  GetRequest,
  GetResponse,
  PinSnapshotRequest,
  ScanRequest,
  ScanResponse,
  SealDatabaseRequest,
  TransactionBeginResponse,
  TransactionGetRequest,
  TransactionScanRequest,
} from "./db-http-api.ts";
import {
  BatchInvalidPublicError,
  type DbApiOperation,
  DatabaseAlreadyExistsPublicError,
  DatabaseNotFoundPublicError,
  DatabaseSealedPublicError,
  InternalFailurePublicError,
  SstDropRefusedPublicError,
  StorageUnavailablePublicError,
  TransactionConflictPublicError,
  UnauthenticatedPublicError,
  WriteStalledPublicError,
} from "./db-public-error.ts";
import { FlushedResponse, WriterStatusResponse } from "./writer-http-api.ts";

/** A value encoded by the schema it is an example of: what the wire carries. */
const encoded = <A, I>(schema: Schema.Codec<A, I>, value: A): I => Schema.encodeSync(schema)(value);

const bytes = (text: string) => new TextEncoder().encode(text);

const ORDERS = DatabaseId.make("orders-eu");
const SST = SstId.make("01JD5Z8X3J4N2K7Q9W1E6R0T5Y");
const CHECKPOINT = CheckpointId.make("01JD5Z9B2C8D4E6F0G1H3J5K7M");
const NOW = Timestamp.make(1_795_660_800_000);
const SNAPSHOT = {
  databaseId: ORDERS,
  manifestId: ManifestId.make(42),
  snapshotSeq: SeqNum.make(1_208),
};
const ROW = {
  key: bytes("user:42"),
  seq: SeqNum.make(1_208),
  value: { _tag: "Value" as const, bytes: bytes('{"name":"Ada"}') },
  createTs: Option.some(NOW),
  expireTs: Option.none(),
};
const USERS_RANGE = { start: bytes("user:"), end: Option.some(bytes("user;")) };

/** One operation's examples: its path and query parameters, request body, and success response. */
interface OperationExamples {
  readonly operation: DbApiOperation;
  readonly parameters?: Readonly<Record<string, string>>;
  readonly request?: unknown;
  readonly success?: unknown;
  /** Examples of the operation's own refusals, by status, beyond the shared ones. */
  readonly errors?: Readonly<Record<string, unknown>>;
}

const databasePath = { databaseId: ORDERS };

/** The public error envelope as an example for `operation`. */
const envelope = (operation: DbApiOperation) => ({
  operation,
  requestId: "8f3a1c2b9d7e6f50-IAD",
});

const OPERATION_EXAMPLES: Readonly<Record<string, OperationExamples>> = {
  "databases.createDatabase": {
    operation: "createDatabase",
    request: encoded(CreateDatabaseRequest, {
      databaseId: ORDERS,
      keyRange: Option.none(),
      mergeOperator: Option.none(),
      changelogRetentionMs: Option.some(86_400_000),
      budgetGroup: Option.none(),
    }),
    success: encoded(CreateDatabaseResponse, { databaseId: ORDERS, manifestId: 1 }),
    errors: {
      "409": encoded(
        DatabaseAlreadyExistsPublicError,
        new DatabaseAlreadyExistsPublicError({
          code: "DatabaseAlreadyExists",
          message: "Database orders-eu already exists. Use it, or choose another id.",
          details: { databaseId: ORDERS },
          retryable: false,
          ...envelope("createDatabase"),
        }),
      ),
    },
  },
  "databases.write": {
    operation: "write",
    parameters: databasePath,
    request: encoded(WriteBatch, {
      operations: [
        {
          _tag: "Put",
          key: bytes("user:42"),
          value: bytes('{"name":"Ada"}'),
          ttlMillis: Option.none(),
        },
        { _tag: "Delete", key: bytes("user:41") },
      ],
    }),
    success: encoded(WriteReceipt, { seq: SeqNum.make(1_208), createTs: NOW }),
    errors: {
      "400": encoded(
        BatchInvalidPublicError,
        new BatchInvalidPublicError({
          code: "BatchInvalid",
          message: "The write batch has no operations. Send at least one.",
          details: { reason: "Empty", operationIndex: null },
          retryable: false,
          ...envelope("write"),
        }),
      ),
      "409": encoded(
        DatabaseSealedPublicError,
        new DatabaseSealedPublicError({
          code: "DatabaseSealed",
          message:
            "Database orders-eu is sealed and accepts no writes; its successors are orders-eu-a, orders-eu-b. Send the write to the successor whose key range holds the key.",
          details: {
            databaseId: ORDERS,
            successors: [DatabaseId.make("orders-eu-a"), DatabaseId.make("orders-eu-b")],
          },
          retryable: false,
          ...envelope("write"),
        }),
      ),
      "503": encoded(
        WriteStalledPublicError,
        new WriteStalledPublicError({
          code: "WriteStalled",
          message:
            "Database orders-eu has 16 L0 SSTs, at its limit, so the write was refused. Retry the same batch after 1000 ms; nothing was written.",
          details: { databaseId: ORDERS, reason: "L0Full", retryAfterMs: 1_000 },
          retryable: true,
          ...envelope("write"),
        }),
      ),
    },
  },
  "databases.flush": {
    operation: "flush",
    parameters: databasePath,
    success: encoded(FlushedResponse, { flushedSeq: SeqNum.make(1_208) }),
  },
  "databases.status": {
    operation: "status",
    parameters: databasePath,
    success: encoded(WriterStatusResponse, {
      databaseId: ORDERS,
      writerEpoch: WriterEpoch.make(3),
      lastSeq: SeqNum.make(1_208),
      flushedSeq: SeqNum.make(1_200),
      retainedFromSeq: SeqNum.make(1_100),
      flushedManifestId: Option.some(ManifestId.make(42)),
      unflushedBytes: 2_048,
      unflushedRows: 8,
      l0Count: 3,
      sealed: false,
    }),
  },
  "databases.compaction": {
    operation: "compaction",
    parameters: databasePath,
    success: encoded(CompactionStatusResponse, {
      manifestId: ManifestId.make(42),
      l0Ssts: 1,
      l0SstIds: [SST],
      sortedRuns: [{ id: SortedRunId.make(7), ssts: 4, sizeBytes: 268_435_456 }],
      compactor: Option.none(),
      corruptSsts: [],
      droppedSsts: [],
    }),
  },
  "databases.dropSst": {
    operation: "dropSst",
    parameters: { databaseId: ORDERS, sstId: SST },
    request: encoded(DropSstRequest, { confirm: SST }),
    success: encoded(SstDropReport, {
      manifestId: ManifestId.make(43),
      dropped: {
        sstId: SST,
        owner: Option.none(),
        from: { _tag: "L0", sstId: SST },
        firstKey: bytes("user:40"),
        lastKey: bytes("user:49"),
        view: Option.none(),
        rowCount: 512,
        reason: "Corrupted",
        droppedAt: NOW,
      },
    }),
    errors: {
      "409": encoded(
        SstDropRefusedPublicError,
        new SstDropRefusedPublicError({
          code: "SstDropRefused",
          message: `Dropping SST ${SST} of database orders-eu loses its rows, so confirm must repeat the SST id. Nothing was dropped; send confirm equal to the SST id to proceed.`,
          details: { databaseId: ORDERS, sstId: SST, reason: "ConfirmationMismatch" },
          retryable: false,
          ...envelope("dropSst"),
        }),
      ),
    },
  },
  "databases.createCheckpoint": {
    operation: "checkpoint",
    parameters: databasePath,
    request: encoded(CreateCheckpointRequest, {
      name: Option.some("before-migration"),
      lifetimeMs: Option.some(604_800_000),
    }),
    success: encoded(Checkpoint, {
      id: CHECKPOINT,
      manifestId: ManifestId.make(42),
      createdAt: NOW,
      expireAt: Option.some(Timestamp.make(NOW + 604_800_000)),
      holder: Option.none(),
      name: Option.some("before-migration"),
    }),
  },
  "databases.listCheckpoints": {
    operation: "checkpoint",
    parameters: databasePath,
    success: encoded(CheckpointsResponse, {
      checkpoints: [
        {
          id: CHECKPOINT,
          manifestId: ManifestId.make(42),
          createdAt: NOW,
          expireAt: Option.none(),
          holder: Option.none(),
          name: Option.some("before-migration"),
        },
      ],
    }),
  },
  "databases.deleteCheckpoint": {
    operation: "checkpoint",
    parameters: { databaseId: ORDERS, checkpointId: CHECKPOINT },
  },
  "databases.seal": {
    operation: "seal",
    parameters: databasePath,
    request: encoded(SealDatabaseRequest, {
      successors: [DatabaseId.make("orders-eu-a"), DatabaseId.make("orders-eu-b")],
    }),
    success: encoded(Schema.Struct({ flushedSeq: SeqNum }), { flushedSeq: SeqNum.make(1_208) }),
  },
  "databases.fork": {
    operation: "fork",
    parameters: databasePath,
    request: encoded(ForkDatabaseRequest, {
      forkId: DatabaseId.make("orders-eu-restored"),
      checkpointId: CHECKPOINT,
      range: { start: new Uint8Array(0), end: Option.none() },
    }),
    success: encoded(CreateDatabaseResponse, {
      databaseId: DatabaseId.make("orders-eu-restored"),
      manifestId: 1,
    }),
  },
  "databases.removeDatabase": { operation: "remove", parameters: databasePath },
  "databases.beginTransaction": {
    operation: "transaction",
    parameters: databasePath,
    success: encoded(TransactionBeginResponse, {
      readSeq: SeqNum.make(1_208),
      manifestId: Option.some(ManifestId.make(42)),
    }),
  },
  "databases.transactionGet": {
    operation: "transaction",
    parameters: databasePath,
    request: encoded(TransactionGetRequest, {
      readSeq: SeqNum.make(1_208),
      manifestId: Option.some(ManifestId.make(42)),
      key: bytes("user:42"),
    }),
    success: encoded(GetResponse, { row: Option.some(ROW) }),
  },
  "databases.transactionScan": {
    operation: "transaction",
    parameters: databasePath,
    request: encoded(TransactionScanRequest, {
      readSeq: SeqNum.make(1_208),
      manifestId: Option.some(ManifestId.make(42)),
      range: USERS_RANGE,
      limit: 100,
    }),
    success: encoded(Schema.Struct({ rows: Schema.Array(RowSchema) }), {
      rows: [ROW],
    }),
  },
  "databases.commitTransaction": {
    operation: "transaction",
    parameters: databasePath,
    request: encoded(TransactionCommit, {
      readSeq: SeqNum.make(1_208),
      isolation: "SerializableSnapshot",
      batch: {
        operations: [
          {
            _tag: "Put",
            key: bytes("user:42"),
            value: bytes('{"name":"Ada Lovelace"}'),
            ttlMillis: Option.none(),
          },
        ],
      },
      readKeys: [bytes("user:42")],
      readRanges: [],
    }),
    success: encoded(WriteReceipt, { seq: SeqNum.make(1_209), createTs: NOW }),
    errors: {
      "409": encoded(
        TransactionConflictPublicError,
        new TransactionConflictPublicError({
          code: "TransactionConflict",
          message:
            "A key this transaction read in database orders-eu was written after it began, so it was not committed. Begin a new transaction and retry.",
          details: { databaseId: ORDERS, reason: "ReadConflict" },
          retryable: true,
          ...envelope("transaction"),
        }),
      ),
    },
  },
  "databases.get": {
    operation: "get",
    parameters: databasePath,
    request: encoded(GetRequest, {
      key: bytes("user:42"),
      consistency: { _tag: "Latest" },
      snapshot: Option.none(),
    }),
    success: encoded(GetResponse, { row: Option.some(ROW) }),
  },
  "databases.scan": {
    operation: "scan",
    parameters: databasePath,
    request: encoded(ScanRequest, {
      range: USERS_RANGE,
      consistency: { _tag: "Flushed" },
      snapshot: Option.none(),
      limit: 100,
    }),
    success: encoded(ScanResponse, { rows: [ROW], resumeFrom: Option.some(bytes("user:43")) }),
  },
  "databases.pinSnapshot": {
    operation: "checkpoint",
    parameters: databasePath,
    request: encoded(PinSnapshotRequest, { lifetimeMs: 3_600_000 }),
    success: encoded(PinnedSnapshot, {
      snapshot: SNAPSHOT,
      checkpointId: CHECKPOINT,
      expireAt: Timestamp.make(NOW + 3_600_000),
    }),
  },
  "databases.snapshot": {
    operation: "checkpoint",
    parameters: databasePath,
    success: encoded(DbSnapshot, SNAPSHOT),
  },
  "databases.changes": {
    operation: "changes",
    parameters: { databaseId: ORDERS, afterSeq: "1200", limit: "100" },
    success: encoded(ChangeFeedResponse, { rows: [ROW], nextAfterSeq: SeqNum.make(1_208) }),
  },
};

/** Examples every operation shares, by status: the refusals any request can meet. */
const sharedErrorExamples = (operation: DbApiOperation): Readonly<Record<string, unknown>> => ({
  "401": encoded(
    UnauthenticatedPublicError,
    new UnauthenticatedPublicError({
      code: "Unauthenticated",
      message: unauthenticatedMessage(LSM_ACCESS_TOKEN_CREDENTIAL),
      details: {},
      retryable: false,
      ...envelope("routeRequest"),
    }),
  ),
  "404": encoded(
    DatabaseNotFoundPublicError,
    new DatabaseNotFoundPublicError({
      code: "DatabaseNotFound",
      message: "Database orders-eu does not exist. Create it first, or check the id.",
      details: { databaseId: ORDERS },
      retryable: false,
      ...envelope(operation),
    }),
  ),
  "500": encoded(
    InternalFailurePublicError,
    new InternalFailurePublicError({
      code: "InternalFailure",
      message:
        "The request could not be completed because of an internal error. Contact the operator with request id 8f3a1c2b9d7e6f50-IAD.",
      details: {},
      retryable: false,
      ...envelope(operation),
    }),
  ),
  "503": encoded(
    StorageUnavailablePublicError,
    new StorageUnavailablePublicError({
      code: "StorageUnavailable",
      message: "Database orders-eu's storage was temporarily unavailable. Retry the same request.",
      details: { databaseId: ORDERS },
      retryable: true,
      ...envelope(operation),
    }),
  ),
});

/** A JSON Schema with one more example (the JSON Schema `examples` keyword). */
const withExample = (schema: object, example: unknown): object => ({
  ...schema,
  examples: [example],
});

/** Adds an operation's examples, and `x-request-id` on each of its responses. */
const annotateOperation = (
  operation: OpenApi.OpenAPISpecOperation,
): OpenApi.OpenAPISpecOperation => {
  const examples = OPERATION_EXAMPLES[operation.operationId];
  if (examples === undefined) return operation;
  const errors = { ...sharedErrorExamples(examples.operation), ...examples.errors };
  const responses: OpenApi.OpenAPISpecResponses = {};
  for (const [status, response] of Object.entries(operation.responses)) {
    const json = response.content?.["application/json"];
    const example = status.startsWith("2") ? examples.success : errors[status];
    responses[Number(status)] = {
      ...response,
      headers: {
        ...response.headers,
        "x-request-id": {
          schema: { type: "string" },
          required: true,
          description:
            "Identifies the request in shaledb's traces and logs; error bodies repeat it as `requestId`.",
        },
      },
      ...(json === undefined || example === undefined
        ? {}
        : {
            content: {
              ...response.content,
              "application/json": { ...json, schema: { ...json.schema, examples: [example] } },
            },
          }),
    };
  }
  const body = operation.requestBody?.content["application/json"];
  return {
    ...operation,
    parameters: operation.parameters.map((parameter) => {
      const example = examples.parameters?.[parameter.name];
      return example === undefined
        ? parameter
        : { ...parameter, schema: withExample(parameter.schema, example) };
    }),
    responses,
    ...(operation.requestBody === undefined || body === undefined || examples.request === undefined
      ? {}
      : {
          requestBody: {
            ...operation.requestBody,
            content: {
              ...operation.requestBody.content,
              "application/json": {
                ...body,
                schema: { ...body.schema, examples: [examples.request] },
              },
            },
          },
        }),
  };
};

/** The methods a path item can hold an operation under. */
const HTTP_METHODS: ReadonlyArray<OpenApi.OpenAPISpecMethodName> = [
  "get",
  "put",
  "post",
  "delete",
  "options",
  "head",
  "patch",
  "trace",
];

/**
 * Completes the generated specification with what the contract's schemas cannot carry (D64): every
 * operation's examples (encoded by the schemas they exemplify, so they conform by construction),
 * the bearer-token scheme, the `x-request-id` header on every response, and the local server.
 */
const completeSpec = (spec: OpenApi.OpenAPISpec): OpenApi.OpenAPISpec => {
  const paths: OpenApi.OpenAPISpecPaths = {};
  for (const [path, item] of Object.entries(spec.paths)) {
    const annotated: OpenApi.OpenAPISpecPathItem = { ...item };
    for (const method of HTTP_METHODS) {
      const operation = item[method];
      if (operation !== undefined) annotated[method] = annotateOperation(operation);
    }
    paths[path] = annotated;
  }
  return {
    ...spec,
    paths,
    servers: [
      {
        url: "http://localhost:1337",
        description:
          "Local development (`vp run dev`), not gated. A live stage's URL is its API Worker's workers.dev URL.",
      },
    ],
    components: {
      ...spec.components,
      securitySchemes: {
        ...spec.components.securitySchemes,
        bearerToken: {
          type: "http",
          scheme: "bearer",
          description:
            "The stage's access token as `Authorization: Bearer <token>`. A live stage accepts either of its two rotatable tokens (`vp run @shaledb/infra#tokens`); a preview also accepts the token `preview-cli token <stage>` prints. Local stages are not gated.",
        },
      },
    },
    security: [{ bearerToken: [] }],
  };
};

/** The public database API's OpenAPI specification, as checked in at `docs/openapi/shaledb-v1.json`. */
export const dbOpenApiSpec = (): OpenApi.OpenAPISpec => completeSpec(OpenApi.fromApi(DbHttpApi));

/** Where the generated specification is checked in, from the repository root. */
export const DB_OPENAPI_SPEC_PATH = "docs/openapi/shaledb-v1.json";
