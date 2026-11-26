import { readdirSync, readFileSync } from "node:fs";
import { dirname, join, relative, resolve, sep } from "node:path";

import { Schema } from "effect";

/** Package name prefix of every workspace in this repository. */
const WORKSPACE_PACKAGE_NAME_PREFIX = "@shaledb/";

/** Package name of the pure LSM core, which must not import Cloudflare or Alchemy. */
const LSM_CORE_PACKAGE_NAME = "@shaledb/lsm-core";

/** Package name of the optional sharding package, which may use only the public library APIs. */
const SHARDING_PACKAGE_NAME = "@shaledb/sharding";

/** Libraries the sharding package may depend on, each through its public entrypoint only. */
const SHARDING_ALLOWED_WORKSPACE_DEPENDENCIES = new Set([
  "@shaledb/lsm-core",
  "@shaledb/lsm-cloudflare",
]);

/** Module specifiers the pure core must never import: Cloudflare runtime and Alchemy modules. */
const CLOUDFLARE_MODULE_PATTERN = /^(?:@alchemy\.run\/|alchemy(?:\/|$)|cloudflare:|@cloudflare\/)/;

const DependencyVersionsByName = Schema.optional(Schema.Record(Schema.String, Schema.String));

/** Schema for the `package.json` fields the boundary rules read; other fields are ignored. */
const WorkspacePackageJson = Schema.fromJsonString(
  Schema.Struct({
    name: Schema.String,
    exports: Schema.optional(Schema.Unknown),
    dependencies: DependencyVersionsByName,
    devDependencies: DependencyVersionsByName,
    peerDependencies: DependencyVersionsByName,
    optionalDependencies: DependencyVersionsByName,
  }),
);

const decodeWorkspacePackageJson = Schema.decodeSync(WorkspacePackageJson);

const DEPENDENCY_FIELDS = [
  "dependencies",
  "devDependencies",
  "peerDependencies",
  "optionalDependencies",
] as const;

const SOURCE_FILE_PATTERN = /\.(?:ts|tsx|mts|js|mjs)$/;

const SKIPPED_DIRECTORY_NAMES = new Set([
  "node_modules",
  "dist",
  ".alchemy",
  ".wrangler",
  ".traces",
  "repos",
]);

/** Matches the specifier of a static `from "…"`, side-effect `import "…"`, or dynamic `import("…")`. */
const IMPORT_PATTERN = /(?:from|import)\s*\(?\s*["']([^"']+)["']/g;

/** Kind of workspace, from its top-level directory. */
export type WorkspaceKind = "infra" | "package" | "tool";

/** Workspace the boundary rules apply to. */
export interface BoundaryWorkspace {
  /** Kind of workspace, which decides whether it may declare `exports` and be depended on. */
  readonly kind: WorkspaceKind;
  /** Workspace directory relative to the repository root, such as `packages/lsm-core`. */
  readonly directory: string;
}

const hasPackageJson = (directory: string): boolean =>
  readdirSync(directory, { withFileTypes: true }).some(
    (entry) => entry.isFile() && entry.name === "package.json",
  );

const childDirectories = (directory: string): ReadonlyArray<string> => {
  try {
    return readdirSync(directory, { withFileTypes: true })
      .filter((entry) => entry.isDirectory() && !SKIPPED_DIRECTORY_NAMES.has(entry.name))
      .map((entry) => join(directory, entry.name));
  } catch {
    return [];
  }
};

/**
 * Lists the workspaces under `infra/`, `packages/`, and `tools/`, matching the globs in
 * `pnpm-workspace.yaml`.
 *
 * @returns Workspaces with directories relative to `repositoryRoot`; a missing top-level directory
 *   contributes none.
 */
export const listBoundaryWorkspaces = (
  repositoryRoot: string,
): ReadonlyArray<BoundaryWorkspace> => {
  const toWorkspaces = (kind: WorkspaceKind, top: string): ReadonlyArray<BoundaryWorkspace> =>
    childDirectories(join(repositoryRoot, top))
      .filter(hasPackageJson)
      .map((directory) => ({ kind, directory: relative(repositoryRoot, directory) }));
  return [
    ...toWorkspaces("infra", "infra"),
    ...toWorkspaces("package", "packages"),
    ...toWorkspaces("tool", "tools"),
  ];
};

const listSourceFiles = (directory: string): ReadonlyArray<string> =>
  readdirSync(directory, { withFileTypes: true }).flatMap((entry) => {
    const path = join(directory, entry.name);
    if (entry.isDirectory()) {
      return SKIPPED_DIRECTORY_NAMES.has(entry.name) ? [] : listSourceFiles(path);
    }
    return entry.isFile() && SOURCE_FILE_PATTERN.test(entry.name) ? [path] : [];
  });

/**
 * Finds every workspace boundary violation in the repository.
 *
 * **Details**
 *
 * The rules, from `docs/coding-standards.md` ("Monorepo Boundaries"):
 *
 * - A workspace under `infra/` declares no `exports`, so no other workspace can import it.
 * - No workspace depends on a workspace under `infra/` or `tools/`.
 * - `@shaledb/lsm-core` neither depends on nor imports a Cloudflare or Alchemy module.
 * - `@shaledb/sharding` depends on no workspace other than `@shaledb/lsm-core` and
 *   `@shaledb/lsm-cloudflare`, and imports each only through its public entrypoint.
 * - No source file reaches outside its own workspace through a relative import.
 *
 * @returns One searchable sentence per violation; empty when the boundaries hold.
 * @throws {Error} A Node.js file system error when a workspace directory or file cannot be read,
 *   or a schema error when a workspace `package.json` lacks a name or has non-string dependency
 *   maps.
 */
export const findWorkspaceBoundaryViolations = (repositoryRoot: string): ReadonlyArray<string> => {
  const violations: Array<string> = [];
  const workspaces = listBoundaryWorkspaces(repositoryRoot).map((workspace) => ({
    ...workspace,
    packageJson: decodeWorkspacePackageJson(
      readFileSync(join(repositoryRoot, workspace.directory, "package.json"), "utf8"),
    ),
  }));
  const kindByPackageName = new Map(
    workspaces.map((workspace) => [workspace.packageJson.name, workspace.kind]),
  );

  for (const workspace of workspaces) {
    const { packageJson } = workspace;
    const workspaceRoot = resolve(repositoryRoot, workspace.directory);

    if (workspace.kind === "infra" && packageJson.exports !== undefined) {
      violations.push(
        `Workspace boundary violation: ${workspace.directory}/package.json declares "exports". ` +
          "Workspaces under infra/ cannot be imported; move shared code to a library under packages/.",
      );
    }

    for (const field of DEPENDENCY_FIELDS) {
      for (const dependencyName of Object.keys(packageJson[field] ?? {})) {
        const dependencyKind = kindByPackageName.get(dependencyName);
        if (dependencyKind === "infra" || dependencyKind === "tool") {
          violations.push(
            `Workspace boundary violation: ${workspace.directory}/package.json lists ` +
              `${dependencyName} in ${field}. Only libraries under packages/ may be depended on; ` +
              "order Stacks with dependsOn in vite.config.ts instead.",
          );
        }
        if (
          packageJson.name === LSM_CORE_PACKAGE_NAME &&
          CLOUDFLARE_MODULE_PATTERN.test(dependencyName)
        ) {
          violations.push(
            `Workspace boundary violation: ${workspace.directory}/package.json lists ` +
              `${dependencyName} in ${field}. The LSM core stays pure; move Cloudflare code to ` +
              "@shaledb/lsm-cloudflare.",
          );
        }
        if (
          packageJson.name === SHARDING_PACKAGE_NAME &&
          dependencyName.startsWith(WORKSPACE_PACKAGE_NAME_PREFIX) &&
          !SHARDING_ALLOWED_WORKSPACE_DEPENDENCIES.has(dependencyName)
        ) {
          violations.push(
            `Workspace boundary violation: ${workspace.directory}/package.json lists ` +
              `${dependencyName} in ${field}. The sharding package may depend only on ` +
              "@shaledb/lsm-core and @shaledb/lsm-cloudflare.",
          );
        }
      }
    }

    for (const sourceFile of listSourceFiles(workspaceRoot)) {
      const source = readFileSync(sourceFile, "utf8");
      const sourcePath = relative(repositoryRoot, sourceFile);
      for (const match of source.matchAll(IMPORT_PATTERN)) {
        const specifier = match[1];
        if (specifier === undefined) continue;
        if (specifier.startsWith("./") || specifier.startsWith("../")) {
          const target = resolve(dirname(sourceFile), specifier);
          if (target !== workspaceRoot && !target.startsWith(workspaceRoot + sep)) {
            violations.push(
              `Workspace boundary violation: ${sourcePath} imports "${specifier}", which is ` +
                `outside ${workspace.directory}. Import the owning library by its package name instead.`,
            );
          }
          continue;
        }
        if (
          packageJson.name === LSM_CORE_PACKAGE_NAME &&
          CLOUDFLARE_MODULE_PATTERN.test(specifier)
        ) {
          violations.push(
            `Workspace boundary violation: ${sourcePath} imports "${specifier}". The LSM core ` +
              "stays pure; move Cloudflare code to @shaledb/lsm-cloudflare.",
          );
        }
        if (
          packageJson.name === SHARDING_PACKAGE_NAME &&
          specifier.startsWith(WORKSPACE_PACKAGE_NAME_PREFIX) &&
          !SHARDING_ALLOWED_WORKSPACE_DEPENDENCIES.has(specifier)
        ) {
          violations.push(
            `Workspace boundary violation: ${sourcePath} imports "${specifier}". The sharding ` +
              "package uses only the public entrypoints of @shaledb/lsm-core and " +
              "@shaledb/lsm-cloudflare.",
          );
        }
      }
    }
  }

  return violations;
};
