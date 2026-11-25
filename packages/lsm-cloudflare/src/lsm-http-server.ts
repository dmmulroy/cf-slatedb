import { Effect, FileSystem, Layer, Path } from "effect";
import { Etag, HttpPlatform } from "effect/http";

/**
 * The platform services Effect's HTTP router needs inside a Worker or Durable Object, which has no
 * file system: response compression and file responses are defects, because cf-slatedb serves only
 * JSON and raw bytes and Cloudflare compresses responses itself.
 */
export const lsmHttpServerLayer = Layer.mergeAll(
  Etag.layer,
  FileSystem.layerNoop({}),
  Path.layer,
  Layer.succeed(HttpPlatform.HttpPlatform, {
    platform: "web",
    compression: {
      algorithms: new Set<HttpPlatform.CompressionAlgorithm>(),
      compressResponse: () =>
        Effect.die(
          "cf-slatedb does not compress responses in a Worker. Return the response uncompressed; Cloudflare compresses it.",
        ),
    },
    fileResponse: () =>
      Effect.die(
        "cf-slatedb Workers have no file system. Return the body as an ordinary response.",
      ),
    fileWebResponse: () =>
      Effect.die(
        "cf-slatedb Workers have no file system. Return the body as an ordinary response.",
      ),
  }),
);
