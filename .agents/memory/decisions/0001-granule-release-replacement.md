# Granule re-release replacement (#47)
> owner calls for #47: release ranking, opt-in replacement, identity in S3 mode
updated: 2026-10-21T11:48:35-04:00

**Status:** accepted

## Context
NASA re-releases GEDI granules within one major version (new PPDS, PGE or generation). Nothing in gedih3 re-indexes a
granule whose file changes. The issue left three questions to the owner (Q1, Q2, Q5); the code settled Q3, Q4 and Q6
(owner comment on #47).

## Decision
- **Ranking (Q1):** PPDS > PGE > generation > suffix, as one pure, unit-tested function. This is the order the old
  path sort plus pivot `aggfunc='last'` in `soc_file_tree` gave implicitly. A suffixed file such as `_SGS` still beats
  the plain file of the same release. A file that ranks *lower* than the one a granule was indexed from is reported,
  never applied (no downgrade).
- **Trigger (Q2):** opt-in. `gh3_build` reports outdated granules; it replaces them only with `--replace-granules`.
  An automatic mode can follow once the replace path has soaked.
- **S3 / CMR (Q5):** the identity is the source basename in every mode (an S3 key's basename is the file name).
  Automatic detection in v1 covers local SOC trees only; S3 builds use the explicit `--replace-granules` list.
- Whole-granule rebuild (Q3, products are inner-joined on `shot_number`); superseded files stay in place (Q4);
  concurrent readers are documented, not locked (Q6).

## Consequences
PR split: explicit ranking (#58, 0.20.9) → identity in the build log (#59, 0.20.10) → detection and doctor diagnosis →
drop-predicate primitive → Stage 0 replace driver.
- Identity is a compact tag per product in the build log (`releases`, `gedi_release_tag`), not a basename: log size.
- The safety rule the reviews converged on: no path may record a release *newer* than the rows it describes. A tag is
  never replaced; an entry registered before #59 stays unknown; a merge keeps a tag only when both logs carry one (older);
  a backfill records its tags before its first write (`releases_callback`). Older-than-rows only costs a needless rebuild.

## Rejected
- PGE first (changes which file wins for granules with mixed PPDS).
- `_SGS` as a non-release suffix (would change today's winner).
- Automatic replacement on every build; CMR detection in v1.
