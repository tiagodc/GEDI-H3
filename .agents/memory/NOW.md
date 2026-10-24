# NOW
from: claude-code/opus-5.5 · 2026-10-24T12:15:00-04:00 · branch main · main at 4b45263c (0.21.3)
## Open questions
- Owner: run `/ship` to release 0.21.3 (last tag is v0.18.1; nothing since is published).
- Owner: open an issue for the mixed column-set drift in [[gotchas/replace-granules-silent-paths]] (status line)?
- Owner: delete leftover scratch (`rm -rf` is denied to agents): `.work/replace_test/{tmp_*,dask_tmp}`, `.work/h3v3_l4c/prof`,
  `/gpfs/data1/vclgp/decontot/temp/dask-local-h3v3`.
## Next steps
0. h3_v3 L4C replacement DONE 2026-10-24 (exit 0, verified). Owner: rebuild DuckLake; triage
   `h3_v3/tmp/maintenance/2026-10-23_l4c/BACKLOG.md` (3 gedih3 PR candidates: replacement scratch peak, re-encoding untouched
   row groups, merge ordering/progress); decide on 1,763 granules indexed from releases no longer in the SOC.
1. Existing V3 databases: one variable add of the L4 inclusion flag for the L4 products they hold
   (`gh3_build -i <soc> -o <db> -l4a degrade_include_flag`, and/or `-l4c ...`). Never name a product the DB lacks.
2. h3_v3 now stores `exclude: ['*_SGS.h5']` and every granule's releases, so later `--replace-granules auto` runs need no extra step.
3. Open issues #69 (old gedih3 on newer DBs) and #70 (var-add memory on dense cells) await the owner.
## Files touched
- `.agents/memory/{NOW,LOG}.md`, `gotchas/replace-granules-silent-paths.md`
## State
No PR open, no worktree, no job running. #64-#66, #71, #72, #73 merged with blind reviews and green CI.
