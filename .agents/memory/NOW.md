# NOW
from: claude-code/opus-5.5 @ gsapp23 · 2026-10-21T15:20:47-04:00 · branch main · main at 2120e552
## Open questions
- Delete the imported Claude harness memory files? Migrate CLAUDE.md -> AGENTS.md? (unanswered)
## Next steps
1. #47 part 5: Stage 0 replace driver behind gh3_build --replace-granules (auto | list), on top of h3_merge_files(replace_granules=).
   See [[decisions/0001-granule-release-replacement]].
2. Ship with /ship when the owner decides (nothing since 0.20.1 is published).
## Files touched
- Merged with a micro bump each: #57 0.20.8, #58 0.20.9, #59 0.20.10, #60 0.20.11, #61 0.20.12.
- Local worktrees ../gedih3-wt-{46,42b,47w,shot,42a,52,56,47a,47b,47c,47d,main} can be removed; branches are squash-merged.
## State
main is at 0.20.12, no PR open. #47 parts 1-4 merged: ranking, release tags, detection (gh3_build warning + doctor
superseded_granules), and the replace_granules merge primitive. #47 stays open for part 5.
