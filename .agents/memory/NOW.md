# NOW
from: claude-code/opus-5.5 @ gsapp23 · 2026-10-20T22:59:03-04:00 · branch agents/project-memory · main at 7138973f
## Open questions
- #47 owner calls before the replace path: release ranking incl. `_SGS` (Q1), auto vs `--replace-granules` (Q2), S3 identity (Q5).
- Delete the imported Claude harness memory files? Migrate CLAUDE.md -> AGENTS.md? (unanswered)
## Next steps
1. Fix #56 (gh3_doctor --fix log_state clears a live _pending_variable_update): own branch, blind review, merge, micro bump.
2. #47 phases A/B (identity + explicit ranking + detection), then C as two PRs, once the questions above are answered.
## Files touched
- Merged 2026-10-20 with a bump each: #49 0.20.2, #50 0.20.3, #54 0.20.4, #55 0.20.5, #51 0.20.6, #53 0.20.7. Not shipped (/ship).
- Local worktrees ../gedih3-wt-{46,42b,47w,shot,42a,52,main} can be removed; their branches are squash-merged.
## State
main is at 0.20.7 with every fix PR merged; issues #42, #46, #52 closed. This memory PR (#48) is the last one open.
Resume markers in the build log (`_pending_update`, `update_ran`, `requested_expansion`) are in src/gedih3/logger.py.
