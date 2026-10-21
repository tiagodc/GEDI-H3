# NOW
from: claude-code/opus-5.5 @ gsapp23 · 2026-10-21T08:26:56-04:00 · branch main · main at ec4be45f
## Open questions
- #47 owner calls before the replace path: release ranking incl. `_SGS` (Q1), auto vs `--replace-granules` (Q2), S3 identity (Q5).
- Delete the imported Claude harness memory files? Migrate CLAUDE.md -> AGENTS.md? (unanswered)
## Next steps
1. #47 phases A/B (identity + explicit ranking + detection), then C as two PRs, once the questions above are answered.
2. Ship 0.20.8 with /ship when the owner decides to release (nothing since 0.20.1 is published).
## Files touched
- Merged with a bump each: #49 0.20.2, #50 0.20.3, #54 0.20.4, #55 0.20.5, #51 0.20.6, #53 0.20.7, #57 0.20.8; #48 no bump.
- Local worktrees ../gedih3-wt-{46,42b,47w,shot,42a,52,56,main} can be removed; their branches are squash-merged.
## State
main is at 0.20.8 with every fix PR merged and no PR open; issues #42, #46, #52, #56 closed. #47 is the only open issue.
Resume markers in the build log are described in [[build-log-resume-state]].
