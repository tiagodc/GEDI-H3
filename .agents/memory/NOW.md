# NOW
from: claude-code/opus-5.5 · 2026-11-12T22:21:43-05:00 · branch main · main at d089e793 (0.21.5 shipped)
## Open questions
- Owner: open an issue for the mixed column-set drift in [[gotchas/replace-granules-silent-paths]] (status line)?
## Next steps
- 0.21.5 SHIPPED 2026-11-12: tag v0.21.5 on d089e793, PyPI, conda-forge (feedstock #11, bot PR within ~2 h).
  Contents: #76 (search_file version match), #77 (variable add reads only added vars; --download keeps full lists),
  #78 (docs: sphinx-design 0.8 card-header; docs extra and environment.yml require >= 0.8).
1. Optional PR: run the docs build (no deploy) on pull_request; today it runs only on push to main, so a new
   sphinx-design release broke every deploy unseen.
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
