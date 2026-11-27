import { createServer } from "node:http";

import { afterAll, beforeAll } from "@effect/vitest";

/**
 * Starts an OTLP collector for the current test file that answers every export at once (test
 * stages flush telemetry before each response, so the Workers need one that answers), and stops
 * it afterwards. The writer stack's delayed collector records spans; this one only answers.
 */
export const useAnsweringCollector = (port: number) => {
  const server = createServer((request, response) => {
    request.resume();
    request.on("end", () => {
      response.writeHead(200, { "content-type": "application/json" });
      response.end('{"partialSuccess":{}}');
    });
  });
  beforeAll(
    () =>
      new Promise<void>((resolve, reject) => {
        server.once("error", reject);
        server.listen(port, "127.0.0.1", resolve);
      }),
  );
  afterAll(() => new Promise<void>((resolve) => server.close(() => resolve())));
};
