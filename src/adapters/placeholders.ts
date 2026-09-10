/**
 * Parses a SQL string with PostgreSQL-style `$N` placeholders into the
 * literal segments between placeholders and the values in textual order.
 *
 * Handles repeated indexes (e.g. `$2` appearing twice) by duplicating the
 * value at each occurrence, so adapters that target positional `?`-style
 * binders or tagged-template SQL builders stay consistent with what
 * postgres would have produced from the original `$N` form.
 */
export function parsePlaceholders (text: string, values?: readonly unknown[]): {
  parts: string[]
  reordered: unknown[]
} {
  const parts: string[] = []
  const reordered: unknown[] = []
  // Local /g regex: stateful via lastIndex but never shared across calls.
  const re = /\$(\d+)/g
  let lastIndex = 0
  let match: RegExpExecArray | null
  while ((match = re.exec(text)) !== null) {
    parts.push(text.slice(lastIndex, match.index))
    reordered.push(values?.[Number(match[1]) - 1])
    lastIndex = re.lastIndex
  }
  parts.push(text.slice(lastIndex))
  return { parts, reordered }
}

// A parameter cast to an array type, as in `$2::uuid[]`. pg-boss binds job ids and queue names
// this way, and the cast is what tells an adapter the value is a postgres array rather than a
// JSON one - a JS array bound to a json column is a JSON array, and rewriting that would change
// what gets stored.
export const ARRAY_CAST = /^::[a-z_][a-z0-9_]*\[\]/i

/**
 * Rewrites `$N` placeholders into a fresh sequential list, expanding any array-cast parameter
 * into an inline `ARRAY[...]` constructor of scalar parameters.
 *
 * Postgres wants an array bound whole, and node-postgres, postgres-js and PGlite all encode one.
 * Bun's SQL client cannot: it stringifies the array, so `= ANY($N::uuid[])` reaches the server as
 * `a,b` and fails with `malformed array literal` (oven-sh/bun#18775). `ARRAY[$2,$3]` is valid
 * everywhere a single array parameter was, and every element stays a bind parameter, so no value
 * is ever escaped into SQL text.
 */
export function expandArrayParams (text: string, values?: readonly unknown[]): {
  text: string
  values: unknown[]
} {
  const { parts, reordered } = parsePlaceholders(text, values)
  const expanded: unknown[] = []
  let rewritten = parts[0]!

  for (let i = 0; i < reordered.length; i++) {
    const value = reordered[i]

    rewritten += Array.isArray(value) && ARRAY_CAST.test(parts[i + 1]!)
      ? `ARRAY[${value.map(element => `$${expanded.push(element)}`).join(',')}]`
      : `$${expanded.push(value)}`

    rewritten += parts[i + 1]!
  }

  return { text: rewritten, values: expanded }
}
