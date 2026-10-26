# --replace-granules: the paths that silently do nothing or the wrong thing
> replace-granules gaps found on real data: pre-0.20.10 logs, products added later, --exclude not persisted
updated: 2026-10-26T11:12:31-04:00

**Status:** all fixed in 0.21.1-0.21.2: 1 by #72 (#67), 2 by #71 (#68), 3 by #64. Open, reported by #73's author and not
independently verified: an update of a DB whose product lacks `root_file_<p>` writes `root_file_<p>`/`root_beam_<p>` into
new cells only (mixed column sets). A DB built with all four products from the start has every column, so it is not exposed.
The core replace path itself passed every real-data test, see [[procedures/replace-granules-real-data-test]].

## Symptom
1. On a database built before 0.20.10 (no `releases` in the log), `--replace-granules auto` prints "No granule to re-index"
   and exits 0. `gh3_doctor --check superseded_granules` reports info "not checked". Explicit keys work, but with no
   downgrade guard (nothing recorded to compare against).
2. A product added after the build by a variable update (e.g. `-l4a agbd` on an L2A+L2B database) has no
   `root_file_<prod>` column, and no tag in `releases`. A re-release of only that product: `auto` never targets it, and an
   explicit key reports SUCCESS and records the new tag while the rows keep the old values (agbd mean 27.05 vs oracle 34.05).
3. `--exclude` was not stored in the build log, and variable adds and the doctor's listings never applied it, so `_SGS`
   variants ranked as "newer releases". Replacing without `--exclude '*_SGS.h5'` targeted every granule with an `_SGS` sibling.

## Cause
1. `H3BuildLogger.superseded_releases` (`src/gedih3/logger.py`) skips granules without a recorded tag (by design, #59).
2. `_stale_granule_rows` (`src/gedih3/utils.py`) skips a product whose source column the batch lacks, and the
   variable-add fan (`_var_fan_granule`, `include_source=False`) never writes that column or records its tag.
3. Exclude patterns were a per-run CLI argument only. Pre-existing since v0.18.1 for variable adds.

## Fix
3: #64 keeps the patterns in the log (`H3BuildLogger.exclude`, union with each run's `--exclude`, saved even by an
up-to-date run or a replacement with nothing to do) and applies them in `build_h3db`, variable adds and the doctor.
A database built earlier has none until one run passes `--exclude '*_SGS.h5'`; an older gedih3 drops them.
1: #72 reads `releases` once from the rows' `root_file_*` columns (`releases_from_rows`), matching by granule key within the version.
2: #71 writes `root_file_<p>`/`root_beam_<p>` for a product a variable add brings in; replacement refuses a product added without them.

## Evidence
Real-data runs on a small test DB (harness in [[procedures/replace-granules-real-data-test]]): scenarios A4, F3, F4, E2,
D1; for #64 X1..X4 (stored exclude, then auto without the flag over `_SGS` decoys: IDENTICAL). A production DB built
before 0.20.10 with `--exclude '*_SGS.h5'` and all four `root_file_*` columns is exposed to 1 and 3, not to 2.
