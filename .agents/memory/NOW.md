# NOW
from: claude-code/opus-5.5 · 2026-10-26T11:43:30-04:00 · branch main · main at d82bfb30+ (0.21.4 shipped)
## Open questions
- Owner: open an issue for the mixed column-set drift in [[gotchas/replace-granules-silent-paths]] (status line)?
## Next steps
- 0.21.4 SHIPPED 2026-10-25: tag v0.21.4, PyPI, conda-forge (feedstock #10; autotick needed the
  `@conda-forge-admin, please update version` issue #9 after 8 h of silence).
1. Optional PR: byte-weighted merge progress + largest-first order for replacements (small gain, fixes an early ETA
   overestimate). Measured not worth it: row-group copy, zstd level. Big replacements: point `-t` at a filesystem with
   room for ~1.7x the rewritten bytes (scratch peak).
2. Existing V3 databases: one variable add of the L4 inclusion flag for the L4 products they hold
   (`gh3_build -i <soc> -o <db> -l4a degrade_include_flag`, and/or `-l4c ...`). Never name a product the DB lacks.
3. A DB that stores `exclude` and every granule's releases needs no extra step for later `--replace-granules auto` runs.
4. Open issues #69 (old gedih3 on newer DBs) and #70 (var-add memory on dense cells) await the owner.
## Files touched
- None uncommitted. Host-specific state, if any: see `.work/host-notes.md` on the machine that ran it.
## State
No PR open, no worktree, no job running.
