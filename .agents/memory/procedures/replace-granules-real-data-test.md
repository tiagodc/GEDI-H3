# Real-data test of --replace-granules against an oracle
> battle-test granule replacement on a small real-data DB: oracle compare, SIGKILL at every phase, resume
updated: 2026-10-26T11:43:10-04:00

The harness lives in `.work/replace_test/` (git-ignored; exists only on the GEDI data host that holds a V003 SOC tree, not in other clones).

1. `make_soc.py`: 9 real V003 granules (2019, 2020, 2022; L2A, L2B, L4A, L4C) copied into a year/doy tree, plus 0-byte
   decoys for each production sibling (V002, `_10algs`, `_SGS`): a decoy that gets read fails loudly.
   Symlinks do not work, because the SOC walker does not follow them. `make_soc.py release <soc>` adds "re-releases":
   copies with a bumped generation, modified in place. Covered: L2A revalued and 2 beams moved 0.4 deg, L2B only,
   all products with the granule leaving the region (it becomes NO_DATA), L4A only, and an older-release decoy.
2. Build the base with the version under test (`gh3.sh <src|head> <soc> <db> <tmp> ...`; for an old release, a
   worktree at its tag or bump commit, e.g. `git worktree add <dir> 5cbed9ac` (0.19.0 has no tag)).
3. Oracle = a fresh HEAD build from the post-release SOC with the same arguments. `compare.py <db> <oracle>` checks, exactly:
   the file set, the rows sorted by shot and every column by name, the metadata (shot_count/range, date_range, granules),
   the log statuses, `h3_partition_ids` and the manifest.
4. Kill suite: `killsuite.sh` (`killrun.py` SIGKILLs the process group at a log pattern + delay), plus `die_at_save.py`
   (an `os._exit` at the final `save_log('COMPLETED')`). Each case resumes with a plain `gh3_build` and is compared.

5. Another source tree: `SRC=<tree>/src KDIR=<dir> ./killsuite_src.sh` (the same 12 cases); `perf_check.sh` = one
   uninterrupted `auto` plus that suite; `excl_check.sh` = stored `--exclude`, then runs without the flag over the decoys.

## Verify
2026-10-21, HEAD 35614abd: scenarios A (0.19.0 base, explicit keys), B (HEAD base, auto), D (variable added before the
replacement), E (no `--exclude`, then abandon and redo) and 14 kill points all came out IDENTICAL to the oracle. A
pre-replacement copy shows 53 differences, so the comparison can fail. The F cases (a product added later) reproduce
[[gotchas/replace-granules-silent-paths]].
2026-10-22: #64 (`excl_check.sh`) and #66 (`perf_check.sh`, before and after its digest fix) all IDENTICAL, 12/12 kill cases.
