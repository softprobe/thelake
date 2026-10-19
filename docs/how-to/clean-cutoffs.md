# Clean cutoffs

Authoritative detail for the `AGENTS.md` clean-cutoff rule. The short invariant
stays in [`AGENTS.md`](../../AGENTS.md); follow this page when replacing an
implementation.

Treat a replacement implementation as a clean cutoff: assume the prior
implementation and its data formats never existed. Do not add database
migrations, compatibility branches, dual-read/write paths, old-format parsers,
data preservation during schema recreation, backfills, fallback behavior, or
regression tests whose purpose is to keep the prior implementation working.
Replace the schema and contracts directly, and update their callers, docs, and
tests together. Add backward compatibility only when the user explicitly asks
for it.
