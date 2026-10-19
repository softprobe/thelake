# Clean cutoffs and no toxic leftovers

Authoritative detail for the consecutive `AGENTS.md` rules on no toxic leftovers
and clean cutoffs. Short invariants stay in [`AGENTS.md`](../../AGENTS.md).

## No toxic leftovers

The codebase is for a reader who has **no context** of how the current shape
was reached. They are not interested in the author's false starts, abandoned
options, migration scars, or "we used to do it this way" archaeology. They care
about the business: what the system does now, and how to change that safely.

Never leave toxic garbage in the repo: dead code paths, compatibility shims,
orphaned docs, duplicate how-tos, temporary files that became permanent,
comments that narrate mistakes, or dual implementations that exist only because
a replacement was incomplete. If a change introduces something, finish the
cutoff—update callers, docs, skills, and tests, and delete what the new shape
replaces—in the same change.

## Clean cutoffs (no compatibility unless asked)

Never add backward compatibility or migrations by default. Treat a replacement
as a clean cutoff: assume the prior implementation and its data formats never
existed. Do not add database migrations, compatibility branches, dual-read/write
paths, old-format parsers, data preservation during schema recreation,
backfills, fallback behavior, or regression tests whose purpose is to keep the
prior implementation working. Replace the schema and contracts directly, and
update their callers, docs, and tests together. Add backward compatibility only
when the user explicitly asks for it.
