import { SchemaAST } from "effect";

/** Check ids that bound a string's, bytes', or array's length (on its decoded side). */
const LENGTH_BOUNDS = new Set([
  "effect/schema/isMaxLength",
  "effect/schema/isLengthBetween",
  "effect/schema/isLength",
  // A ULID is exactly 26 characters.
  "effect/schema/isULID",
]);

const checkIds = (checks: SchemaAST.Checks | undefined): ReadonlyArray<string> =>
  (checks ?? []).flatMap((check) => {
    const representation = check.annotations?.["representation"];
    const id =
      typeof representation === "object" && representation !== null && "id" in representation
        ? [String(representation.id)]
        : [];
    return check._tag === "FilterGroup" ? [...id, ...checkIds(check.checks)] : id;
  });

const bounded = (ast: SchemaAST.AST) => checkIds(ast.checks).some((id) => LENGTH_BOUNDS.has(id));

/**
 * The paths of every string, bytes, or array in `ast` without a length bound, and of every record
 * (an index signature has no count bound). Request schemas must return none (D100).
 */
export const unboundedFields = (ast: SchemaAST.AST, path = "$"): ReadonlyArray<string> => {
  // Only a recursive schema can revisit a node; the same node elsewhere is checked at each place.
  const suspended = new Set<SchemaAST.AST>();
  const walk = (node: SchemaAST.AST, at: string): ReadonlyArray<string> => {
    switch (node._tag) {
      case "String":
        return bounded(node) ? [] : [`${at} (string)`];
      case "Declaration":
        if (node.typeParameters.length === 0) return bounded(node) ? [] : [`${at} (bytes)`];
        return node.typeParameters.flatMap((parameter) => walk(parameter, at));
      case "Arrays":
        return [
          ...(bounded(node) || node.rest.length === 0 ? [] : [`${at} (array)`]),
          ...node.elements.flatMap((element, index) => walk(element, `${at}[${index}]`)),
          ...node.rest.flatMap((element) => walk(element, `${at}[]`)),
        ];
      case "Objects":
        return [
          ...node.indexSignatures.map(() => `${at} (record)`),
          ...node.propertySignatures.flatMap((property) =>
            walk(property.type, `${at}.${String(property.name)}`),
          ),
        ];
      case "Union":
        return node.types.flatMap((member) => walk(member, at));
      case "Suspend": {
        if (suspended.has(node)) return [];
        suspended.add(node);
        return walk(node.thunk(), at);
      }
      default:
        return [];
    }
  };
  return walk(ast, path);
};

/** What {@link unboundedRequestFields} reads of an endpoint's request schemas. */
interface RequestSchemas {
  readonly method: string;
  readonly path: string;
  readonly params: { readonly ast: SchemaAST.AST } | undefined;
  readonly query: { readonly ast: SchemaAST.AST } | undefined;
  readonly headers: { readonly ast: SchemaAST.AST } | undefined;
  readonly payload: ReadonlyMap<
    string,
    { readonly schemas: ReadonlyArray<{ readonly ast: SchemaAST.AST }> }
  >;
}

/** Every unbounded request field of every endpoint of `api`: params, query, headers, payloads. */
export const unboundedRequestFields = (api: {
  readonly groups: Readonly<
    Record<string, { readonly endpoints: Readonly<Record<string, RequestSchemas>> }>
  >;
}): ReadonlyArray<string> =>
  Object.values(api.groups).flatMap((group) =>
    Object.values(group.endpoints).flatMap((endpoint) => {
      const where = `${endpoint.method} ${endpoint.path}`;
      const parts = [
        ...(endpoint.params === undefined ? [] : [["params", endpoint.params.ast] as const]),
        ...(endpoint.query === undefined ? [] : [["query", endpoint.query.ast] as const]),
        ...(endpoint.headers === undefined ? [] : [["headers", endpoint.headers.ast] as const]),
        ...[...endpoint.payload.values()].flatMap((payload) =>
          payload.schemas.map((schema) => ["payload", schema.ast] as const),
        ),
      ];
      return parts.flatMap(([part, ast]) => unboundedFields(ast, `${where} ${part}`));
    }),
  );
