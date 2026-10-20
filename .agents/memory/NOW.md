# NOW
from: claude-code/opus-5.5 @ gsapp23 · 2026-10-20T15:25:28-04:00 · branch agents/project-memory · last commit 10ce7b8b
## Open questions
- #47 owner calls before the replace path: release ranking incl. `_SGS` (Q1), auto vs `--replace-granules` (Q2), S3 identity (Q5).
- Delete the imported Claude harness memory files? Migrate CLAUDE.md -> AGENTS.md? (unanswered)
## Next steps
1. Owner merges (admin squash, see [[procedures/landing-prs-on-main]]): #48, #49 (P0, #46), #51, #53, then #50.
2. After #50 squash-merges: retarget #54 and #55 to main (`gh pr edit N --base main`), sync each with origin/main, merge one, then
   sync the other: they conflict in `H3BuildLogger.__init__` (#54 adds `requested_expansion` before the restore call, #55
   renames it to `_restore_pending_update`) and in `to_dict`'s marker condition. Every PR adds `## [Unreleased]`: merge entries.
3. #47 phases A/B (identity + explicit ranking + detection), then C as two PRs; needs the open questions above.
4. Pre-existing, not filed: doctor `log_state` fix clearing a stuck `_pending_variable_update` drops a mixed update's Phase 2.
## Files touched
- PR branches (all pushed): fix/merge-keep-dest-until-promote (#49), fix/resume-interrupted-spatial-expansion (#50),
  feat/warn-superseded-granule-files (#51), fix/gedishot-decoding (#53), fix/expansion-defers-unindexed-granules (#54, on #50),
  fix/resume-interrupted-variable-update (#55, on #50). Local worktrees ../gedih3-wt-{46,42b,47w,shot,42a,52} can be removed.
## State
All six fix PRs are open, blind-reviewed (Sonnet high / Opus medium), review findings fixed and summarized in PR comments;
fast suites green on every branch (1338-1351 passed). Issues #42, #46, #52 close with their PRs; #47 stays open. Nothing merged.
