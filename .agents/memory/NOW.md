# NOW
from: claude-code/opus-5.5 @ gsapp23 · 2026-10-21T17:27:40-04:00 · branch main · main at 4a1e80f8
## Open questions
- Delete the imported Claude harness memory files? Migrate CLAUDE.md -> AGENTS.md? (unanswered)
## Next steps
1. Ship with /ship when the owner decides (nothing since 0.20.1 is published; main is 0.21.0).
2. Possible follow-ups to #47, not filed: automatic replacement mode (Q2 later), S3/CMR detection (Q5 later).
## Files touched
- Merged this round: #57 0.20.8, #58 0.20.9, #59 0.20.10, #60 0.20.11, #61 0.20.12, #62 0.21.0.
- Local worktrees ../gedih3-wt-{46,42b,47w,shot,42a,52,56,47a,47b,47c,47d,47e,main} can be removed; branches are squash-merged.
## State
main is at 0.21.0; no PR or issue open. #47 done in five PRs (#58-#62): ranking, release tags, detection,
replace_granules merge primitive, gh3_build --replace-granules. See [[decisions/0001-granule-release-replacement]].
