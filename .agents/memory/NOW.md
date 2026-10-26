# NOW
from: claude-code/opus-5.5 · 2026-10-25T06:02:55-04:00 · branch main · main at 7d5b764d+ (0.21.4 shipped)
## Open questions
- Owner: open an issue for the mixed column-set drift in [[gotchas/replace-granules-silent-paths]] (status line)?
- Owner: delete leftover scratch (`rm -rf` is denied to agents): `.work/replace_test/{tmp_*,dask_tmp}`, `.work/h3v3_l4c/prof`,
  `.work/pr_eval/{tb,tbb,frag_out}` (16 GB), `.work/review_74`, `/gpfs/data1/vclgp/decontot/temp/dask-local-h3v3`.
## Next steps
- 0.21.4 SHIPPED 2026-10-25: tag v0.21.4, PyPI, conda-forge (feedstock #10, fef3bb54; autotick needed the
  `@conda-forge-admin, please update version` issue #9 after 8 h of silence).
0. h3_v3 L4C replacement DONE 2026-10-24 (exit 0, verified). DuckLake rebuild handed to someone else (2026-10-25; catalog still the Oct 2 one). Owner: decide on 1,763 granules indexed
   from releases no longer in the SOC.
1. h3_v3 backlog PR candidates validated locally 2026-10-24 (see LOG). Done: #74 (merge drift check + shot dedup, ~2x on
   large replacement merges). Optional: byte-weighted merge progress + largest-first order for replacements (small, ~0.7%
   time, fixes a 3x early ETA). Not worth it: row-group copy (~4-5% copyable bytes), zstd level. Scratch peak: use `-t` on
   moggp/tandemxgp for big replacements; per-year batching only if a replacement outgrows every fileset.
2. Existing V3 databases: one variable add of the L4 inclusion flag for the L4 products they hold
   (`gh3_build -i <soc> -o <db> -l4a degrade_include_flag`, and/or `-l4c ...`). Never name a product the DB lacks.
3. h3_v3 now stores `exclude: ['*_SGS.h5']` and every granule's releases, so later `--replace-granules auto` runs need no extra step.
4. Open issues #69 (old gedih3 on newer DBs) and #70 (var-add memory on dense cells) await the owner.
## Files touched
- `.agents/memory/NOW.md`, `.agents/memory/log/`
## State
No PR open, no worktree, no job running. Release cycle closed. #64-#66, #71-#74 merged with blind reviews and green CI.
