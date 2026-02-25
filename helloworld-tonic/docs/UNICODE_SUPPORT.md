# Unicode Support (Parked)

This project currently implements and validates prefixes/words as ASCII `a-z` only.
Unicode support is intentionally deferred so we can first optimize the single-node hot path.

## Current Behavior

- Input normalization uses ASCII lowercasing (`to_ascii_lowercase()`).
- Store validation enforces `a-z` only for words and prefixes.
- Trie node fanout is fixed at 26 and indexes are computed by `('a'..'z')` offsets.

## Proposed Unicode Plan (Future)

### Semantics

- Case-insensitive matching via Unicode casefold.
- Prefix boundary uses Unicode scalar values (`char`) (not grapheme clusters).

### Workflow

1. Introduce a single canonical `normalize(s: &str) -> String` applied on both ingest and query.
2. Maintain two internal indices per tenant:
   - `AsciiTrie` fast path for normalized `[a-z]+`.
   - `UnicodeTrie` for all other normalized words/prefixes.
3. Query routing:
   - ASCII prefix queries hit `AsciiTrie` (and optionally `UnicodeTrie` if complete results are required).
   - Non-ASCII prefix queries hit `UnicodeTrie`.

### Notes

- `UnicodeTrie` should avoid per-step allocations during traversal (push/pop buffer) and must stop early at `top_k`.
- Decide explicitly whether to apply NFKC in addition to casefold (tradeoff: matchability vs semantic changes).

