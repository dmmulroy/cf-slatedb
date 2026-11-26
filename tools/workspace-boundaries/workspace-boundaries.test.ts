import { mkdirSync, mkdtempSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";

import { describe, expect, it } from "vite-plus/test";

import { findWorkspaceBoundaryViolations, listBoundaryWorkspaces } from "./workspace-boundaries.ts";

const REPOSITORY_ROOT = resolve(import.meta.dirname, "..", "..");

/** Writes a throwaway repository layout and returns its root. */
const makeFakeRepository = (files: Readonly<Record<string, string>>): string => {
  const root = mkdtempSync(join(tmpdir(), "workspace-boundaries-"));
  for (const [path, contents] of Object.entries(files)) {
    mkdirSync(dirname(join(root, path)), { recursive: true });
    writeFileSync(join(root, path), contents);
  }
  return root;
};

/** Builds an import statement at runtime, so this file's own source holds no import of `specifier`. */
const importLine = (bindings: string, specifier: string) =>
  ["import", bindings, "from", `"${specifier}";`].join(" ");

const packageJson = (fields: Record<string, unknown>) => JSON.stringify(fields);

describe("workspace boundaries", () => {
  it("hold for every workspace in this repository", () => {
    expect(listBoundaryWorkspaces(REPOSITORY_ROOT).length).toBeGreaterThan(0);
    expect(findWorkspaceBoundaryViolations(REPOSITORY_ROOT)).toEqual([]);
  });

  it("reports an infra workspace that declares exports", () => {
    const root = makeFakeRepository({
      "infra/shaledb/package.json": packageJson({
        name: "@shaledb/infra",
        exports: { ".": "./src/index.ts" },
      }),
    });

    expect(findWorkspaceBoundaryViolations(root)).toEqual([
      expect.stringContaining('infra/shaledb/package.json declares "exports"'),
    ]);
  });

  it("reports a dependency on an infra or tools workspace", () => {
    const root = makeFakeRepository({
      "infra/shaledb/package.json": packageJson({ name: "@shaledb/infra" }),
      "tools/trace-cli/package.json": packageJson({ name: "@shaledb/tools-trace-cli" }),
      "packages/lsm-cloudflare/package.json": packageJson({
        name: "@shaledb/lsm-cloudflare",
        dependencies: { "@shaledb/infra": "workspace:*" },
        devDependencies: { "@shaledb/tools-trace-cli": "workspace:*" },
      }),
    });

    expect(findWorkspaceBoundaryViolations(root)).toEqual([
      expect.stringContaining("lists @shaledb/infra in dependencies"),
      expect.stringContaining("lists @shaledb/tools-trace-cli in devDependencies"),
    ]);
  });

  it("reports a Cloudflare import or dependency in the pure core", () => {
    const root = makeFakeRepository({
      "packages/lsm-core/package.json": packageJson({
        name: "@shaledb/lsm-core",
        dependencies: { alchemy: "catalog:" },
      }),
      "packages/lsm-core/src/row.ts": [
        importLine("{ Effect }", "effect"),
        importLine("* as Cloudflare", "@alchemy.run/cloudflare"),
        importLine("{ DurableObject }", "cloudflare:workers"),
      ].join("\n"),
    });

    expect(findWorkspaceBoundaryViolations(root)).toEqual([
      expect.stringContaining("lists alchemy in dependencies"),
      expect.stringContaining('imports "@alchemy.run/cloudflare"'),
      expect.stringContaining('imports "cloudflare:workers"'),
    ]);
  });

  it("reports a sharding import of a non-public path or another workspace", () => {
    const root = makeFakeRepository({
      "packages/lsm-sharding/package.json": packageJson({
        name: "@shaledb/sharding",
        dependencies: { "@shaledb/lsm-cloudflare": "workspace:*" },
      }),
      "packages/lsm-sharding/src/shard-router.ts": [
        importLine("{ DbClient }", "@shaledb/lsm-cloudflare"),
        importLine("{ MemTable }", "@shaledb/lsm-cloudflare/src/mem-table.ts"),
      ].join("\n"),
    });

    expect(findWorkspaceBoundaryViolations(root)).toEqual([
      expect.stringContaining('imports "@shaledb/lsm-cloudflare/src/mem-table.ts"'),
    ]);
  });

  it("reports a relative import that leaves its workspace", () => {
    const root = makeFakeRepository({
      "packages/lsm-cloudflare/package.json": packageJson({ name: "@shaledb/lsm-cloudflare" }),
      "packages/lsm-cloudflare/src/db-reader.ts": [
        importLine("{ a }", "./local.ts"),
        importLine("{ b }", "../../lsm-core/src/row.ts"),
      ].join("\n"),
    });

    expect(findWorkspaceBoundaryViolations(root)).toEqual([
      expect.stringContaining('imports "../../lsm-core/src/row.ts"'),
    ]);
  });
});
