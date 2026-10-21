# Granule re-release replacement (#47)
> owner calls for #47: release ranking, opt-in replacement, identity in S3 mode
updated: 2026-10-21T08:31:11-04:00

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
PR split: explicit ranking → identity in the build log → detection and doctor diagnosis → drop-predicate primitive →
Stage 0 replace driver.

## Rejected
- PGE first (changes which file wins for granules with mixed PPDS).
- `_SGS` as a non-release suffix (would change today's winner).
- Automatic replacement on every build; CMR detection in v1.
