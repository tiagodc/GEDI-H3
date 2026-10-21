# --replace-granules: the paths that silently do nothing or the wrong thing
> replace-granules gaps found on real data: pre-0.20.10 logs, products added later, --exclude not persisted
updated: 2026-10-21T19:33:28-04:00

**Status:** active (0.21.0). The core replace path itself passed every real-data test, see [[procedures/replace-granules-real-data-test]].

## Symptom
1. On a database built before 0.20.10 (no `releases` in the log), `--replace-granules auto` prints "No granule to re-index"
   and exits 0. `gh3_doctor --check superseded_granules` reports info "not checked". Explicit keys work, but with no
   downgrade guard (nothing recorded to compare against).
2. A product added after the build by a variable update (e.g. `-l4a agbd` on an L2A+L2B database) has no
   `root_file_<prod>` column, and no tag in `releases`. A re-release of only that product: `auto` never targets it, and an
   explicit key reports SUCCESS and records the new tag while the rows keep the old values (agbd mean 27.05 vs oracle 34.05).
3. `--exclude` is not stored in the build log. `_build_add_variables` (`src/gedih3/gh3builder.py:5032`), the doctor backfill
   listing (`src/gedih3/doctor/diagnoses/backfill.py:297`) and the doctor superseded check never apply it, so `_SGS` variants
   rank as "newer releases". Replacing without `--exclude '*_SGS.h5'` targets every granule with an `_SGS` sibling.

## Cause
1. `H3BuildLogger.superseded_releases` (`src/gedih3/logger.py:1475`) skips granules without a recorded tag (by design, #59).
2. `_stale_granule_rows` (`src/gedih3/utils.py:1936`) skips a product whose source column the batch lacks, and the
   variable-add fan (`_var_fan_granule`, `include_source=False`) never writes that column or records its tag.
3. Exclude patterns are a per-run CLI argument only. Pre-existing since v0.18.1 for variable adds.

## Fix
Not fixed yet. Proposed: refuse a replacement when `h3_columns` lacks `root_file_<p>` for a mapped product (a-priori, from
the log); record tags in the variable-add path; persist `exclude` in the build log and apply it in every SOC listing; a
releases backfill from partition `root_file_*` columns for pre-0.20.10 logs.

## Evidence
Real-data runs under `.work/replace_test/` (git-ignored): logs `A4_auto.log`, `F3_auto.log`, `F4_explicit.log`, `E2_auto.log`,
`D1_varadd.log`. Production h3_v3 (0.19.0 build, all four `root_file_*` columns, built with `--exclude '*_SGS.h5'`, 33,764
L2A granules with an `_SGS` sibling) is exposed to 1 and 3, not to 2.
