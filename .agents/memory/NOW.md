# NOW
from: claude-code/opus-5.5 · 2026-10-20T12:43:10-04:00 · branch agents/project-memory · last commit 0b023ec1
## Open questions
- #47 owner calls: release ranking incl. `_SGS` suffix (Q1), auto vs `--replace-granules` flag (Q2), S3 identity (Q5).
- Delete the imported Claude harness memory files? Migrate CLAUDE.md → AGENTS.md? (both unanswered)
## Next steps
1. PR: fix #46 (h3_merge_files rm_src=False + delete fragments after os.replace; sweep promotes a valid orphan .parquet.tmp). Small, P0.
2. PR: #42 case 2 (persisted pending-expansion marker in logger; logger-level test). Medium, P1.
3. PR: soc_file_tree duplicate-release warning (from #47 section 4). Small, P1.
4. PR: #42 case 1 (CLI: never-indexed PENDING granules read under the union scope). Medium-high, P2.
5. PR: GEDIShot/GEDIFile.search_file cleanup (public API; CHANGELOG note). Small, P3.
6. #47 phases A/B, then C as two PRs (merge primitive, then Stage 0 driver). Large, after #46 + #42.
## Files touched
- .agents/memory/* (committed on agents/project-memory, PR #48)
## State
Audited issues #42/#46/#47 on main 13e820a4 and posted verification comments on each (2026-10-20). #46 reproduced by a
scratch test (orphan tmp holds all rows; deleted by the next merge to that partition-year). #42 case 2 reproduced at
logger level; no synthetic-HDF5 gh3_build fixture exists, so logger-level tests are the practical gate. PR #48 open, CI pending.
