# NOW
from: claude-code/opus-5.5 @ gsapp23 · 2026-10-21T18:42:10-04:00 · branch main · main at aaa89339
## Open questions
- Delete the merged PR branches on GitHub (origin/fix/*, origin/feat/*)? Not asked yet.
## Next steps
1. Ship with /ship when the owner decides (nothing since 0.20.1 is published; main is 0.21.0).
2. Possible follow-ups to #47, not filed: automatic replacement mode (Q2 later), S3/CMR detection (Q5 later).
## Files touched
- Merged this round: #57 0.20.8, #58 0.20.9, #59 0.20.10, #60 0.20.11, #61 0.20.12, #62 0.21.0.
- Worktrees and local branches of merged PRs removed. #63 moved CLAUDE.md to AGENTS.md (CLAUDE.md imports it).
## State
main is at 0.21.0; no PR or issue open. #47 done in five PRs (#58-#62): ranking, release tags, detection,
replace_granules merge primitive, gh3_build --replace-granules. See [[decisions/0001-granule-release-replacement]].
