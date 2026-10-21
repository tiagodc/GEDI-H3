# NOW
from: claude-code/opus-5.5 @ gsapp23 · 2026-10-21T19:45:00-04:00 · branch main · main at 35614abd
## Open questions
- Published PyPI/conda-forge is 0.18.1 (not 0.20.1). Ship 0.21.0 now, or first fix the gaps in [[gotchas/replace-granules-silent-paths]]?
- Which granules does the owner mean to replace in production h3_v3? It is a 0.19.0 build with no recorded releases, so `auto` is a no-op there.
- Delete the merged PR branches on GitHub (origin/fix/*, origin/feat/*)? Not asked yet.
## Next steps
1. Owner decision on the fixes: refuse a replacement when `root_file_<p>` is missing; persist `--exclude` in the build log
   and apply it in every listing; record tags in the variable-add path; backfill releases from `root_file_*` for old logs.
2. Perf follow-ups from the audit: give each merge task only its own subset of the replace map (`src/gedih3/gh3builder.py:4160`);
   locate a granule's cell-years a priori instead of reading every cell × year sidecar (`:5537`).
3. Before a production replacement: one run holding every target, with `--exclude '*_SGS.h5'`; rebuild DuckLake afterwards.
## Files touched
- .agents/memory/{NOW,INDEX,LOG}.md, gotchas/replace-granules-silent-paths.md, procedures/replace-granules-real-data-test.md
- `.work/replace_test/` (git-ignored): real-data fixture, about 30 GB of SOC copies and test DBs; delete when done.
- `.work/wt_0190`: a git worktree at 0.19.0 (remove with `git worktree remove .work/wt_0190`).
## State
The audit of 0.18.1 → 0.21.0 is done. The core replace path is exact and kill-safe on real data. The compat and perf reviewer
reports are in `.work/replace_test/reports/` (git-ignored). No code change and no running job.
