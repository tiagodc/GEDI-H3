# NOW
from: claude-code/opus-5.5 · 2026-10-22T08:41:07-04:00 · branch main · main at fe2b9a44
## Open questions
- Owner: merge #64, #65, #66 (all green, independently reviewed). Auto mode refused the `--admin` merge, see [[procedures/landing-prs-on-main]] step 8.
- Owner: decide on #67 (releases backfill for pre-0.20.10 DBs, needed for `auto` on h3_v3), #68 (variable-add provenance;
  the minimal refuse-guard is ~2 h), #69 (old gedih3 on newer DBs), #70 (var-add memory on dense cells).
- Delete `.work/replace_test/` (~30 GB) and worktrees `.work/wt_0190`, `.work/wt_0181`? Delete merged PR branches on GitHub?
## Next steps
1. Merge in this order, each with the bump-version skill (micro) right after, pushed to main:
   #64 → 0.21.1; then sync #65 with origin/main (CHANGELOG `[Unreleased]` conflict: keep only its own entry, step 4 of the
   landing procedure), CI, merge → 0.21.2; same for #66 → 0.21.3.
2. Before any replacement on h3_v3: `gh3_build -i <soc> -o <db> --exclude '*_SGS.h5'` once on 0.21.1+ (stores the pattern),
   then explicit keys (no `auto` until #67), one run holding every target, then rebuild DuckLake.
3. Branches of #64/#65/#66 need `git branch -D` locally after their squash merges.
## Files touched
- `.agents/memory/{NOW,LOG}.md`, `gotchas/replace-granules-silent-paths.md`, `procedures/{landing-prs-on-main,replace-granules-real-data-test}.md`
- PR branches: `fix/persist-exclude` (#64), `fix/build-h3db-positional` (#65, worktree `.work/wt_sig`), `perf/replace-map-from-journal` (#66, worktree `.work/wt_perf`)
- `.work/replace_test/` new scripts `excl_check.sh`, `perf_check.sh`, `killsuite_src.sh`; review scratch `.work/review_6{4,5,6}/`
## State
Items 1, 4, 5 of the audit are done as #64-#66, each with tests that fail without the fix, a blind review whose
should-fixes were applied, a green fast suite and green CI (#66's CI reran after its last commit). Nothing is merged and
no version was bumped. No job is running.
