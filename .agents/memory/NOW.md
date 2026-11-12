# NOW
from: claude-code/opus-5.5 · 2026-11-12T16:18:11-05:00 · branch main · main at b90bfd75 (0.21.5 bumped, not shipped)
## Open questions
- Owner: open an issue for the mixed column-set drift in [[gotchas/replace-granules-silent-paths]] (status line)?
## Next steps
1. Ship 0.21.5 (`/ship`, owner): #76 (search_file matches the root file's GEDI version, #75) and #77 (a variable add
   reads only the added variables; `--download` still requests each product's full stored list).
2. Phased V3 update: add variables first (one variable-add run), then the L2A+L2B granules with
   `--allow-missing-products`, then a plain run to backfill L4A/L4C. See [[gotchas/variable-add-scope]].
3. Existing V3 databases: one variable add of the L4 inclusion flag for the L4 products they hold
   (`gh3_build -i <soc> -o <db> -l4a degrade_include_flag`, and/or `-l4c ...`). Never name a product the DB lacks.
4. Open issues #69 (old gedih3 on newer DBs) and #70 (var-add memory on dense cells; large backfills) await the owner.
5. Optional PR: byte-weighted merge progress + largest-first order for replacements (small gain).
## Files touched
- None uncommitted. Host-specific state, if any: see `.work/host-notes.md` on the machine that ran it.
## State
No PR open, no worktree, no job running.
