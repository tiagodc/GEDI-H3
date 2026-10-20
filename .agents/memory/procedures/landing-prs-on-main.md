# Landing PRs on main
> main is review-protected: owner admin-squash-merges; agents open PRs, never merge unasked
updated: 2026-10-20T11:45:23-04:00

Repo `tiagodc/GEDI-H3`. `main` requires one approving code-owner review; the owner can't approve their own PRs,
so PRs land via `gh pr merge N --squash --admin`, squash subject `<PR title> (#N)`.

Agent policy (owner's instruction): branch-level git/gh work on PR branches is fine (commit, push, `gh pr create/edit`,
forward-merge stacked branches). Never `gh pr merge` and never push to main without an explicit request.
Admin merges bypass review, so harness safety classifiers block them unless the user asked in that session.

1. Branch off `origin/main`; one focused change per PR; Conventional Commit subject (see recent `git log`).
2. For a stacked PR: keep each branch current with `origin/main` and wait for CI before merging.
3. After the base squash-merges, retarget the dependent PR: `gh pr edit N --base main`.
4. Syncing the next stacked branch with `origin/main` then conflicts (squash vs original commits). Merge the updated base
   branch in first, then check `git rev-parse origin/main^{tree}` equals the base tip's tree. If equal,
   `git merge -s ours origin/main` is exact. Verified on #38→#40 (2026-10-05).
5. Squash-merged branches need `git branch -D`.
6. For a tricky stack, rehearse the whole merge sequence in a scratch worktree and compare trees with `git diff --stat`.

## Verify
- `git diff origin/main HEAD --stat` on a synced stacked branch lists only that PR's own files.
- Don't treat a green `claude-review` as an independent review: see [[gotchas/claude-review-check-noop]].
