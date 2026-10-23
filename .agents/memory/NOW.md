# NOW
from: claude-code/opus-5.5 · 2026-10-22T22:09:31-04:00 · branch main · main at 4b45263c (0.21.3)
## Open questions
- Owner: run `/ship` to release 0.21.3 (last tag is v0.18.1; nothing since is published).
- Owner: open an issue for the mixed column-set drift in [[gotchas/replace-granules-silent-paths]] (status line)?
- Owner: delete scratch under `.work/` (`review_6{7a,8a}`, `review_7{1,2,3,3b}`, `qf_scratch`, `replace_test/` incl. `db_F68`,
  `db_A67*`, `hold68`, `tmp`, `logs`); `rm -rf` is denied to agents.
## Next steps
1. Existing V3 databases: one variable add of the L4 inclusion flag for the L4 products they hold
   (`gh3_build -i <soc> -o <db> -l4a degrade_include_flag`, and/or `-l4c ...`). Never name a product the DB lacks.
2. h3_v3 before any replacement: `gh3_build -i <soc> -o <db> --exclude '*_SGS.h5'` once (stores the pattern), then
   `--replace-granules auto` works (releases are read from rows on the first run), then rebuild DuckLake.
3. Open issues #69 (old gedih3 on newer DBs) and #70 (var-add memory on dense cells) await the owner.
## Files touched
- `.agents/memory/{NOW,LOG}.md`, `gotchas/replace-granules-silent-paths.md`
## State
No PR open, no worktree, no job running. #64-#66, #71, #72, #73 merged with blind reviews and green CI.
