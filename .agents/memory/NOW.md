# NOW
from: claude-code/opus-5.5 @ gsapp23 · 2026-10-21T11:48:35-04:00 · branch main · main at 6a0fb64d
## Open questions
- Delete the imported Claude harness memory files? Migrate CLAUDE.md -> AGENTS.md? (unanswered)
## Next steps
1. #47 part 3: detection (listing tag vs recorded `releases`, newer → report, older → no-downgrade report) in gh3_build
   plus a read-only gh3_doctor diagnosis. See [[decisions/0001-granule-release-replacement]].
2. #47 part 4 (drop-predicate primitive in h3_merge_files / parquet_merge_files), then part 5 (Stage 0 behind --replace-granules).
3. Ship with /ship when the owner decides (nothing since 0.20.1 is published).
## Files touched
- Merged with a micro bump each: #57 0.20.8, #58 0.20.9, #59 0.20.10.
- Local worktrees ../gedih3-wt-{46,42b,47w,shot,42a,52,56,47a,47b,main} can be removed; their branches are squash-merged.
## State
main is at 0.20.10, no PR open. #47 parts 1-2 are merged (ranking, release tags in the build log); #47 stays open.
