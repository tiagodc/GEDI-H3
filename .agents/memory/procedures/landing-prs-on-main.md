# Landing PRs on main
> main is review-protected: owner admin-squash-merges; agents open PRs, never merge unasked
updated: 2026-10-22T08:41:07-04:00

Repo `tiagodc/GEDI-H3`. `main` requires one approving code-owner review; the owner can't approve their own PRs,
so PRs land via `gh pr merge N --squash --admin --subject "<PR title> (#N)"`. `enforce_admins` is off, so the
owner's bump commits are pushed straight to main (`git push origin HEAD:main`).

Agent policy (owner's instruction): branch-level git/gh work on PR branches is fine (commit, push, `gh pr create/edit`,
forward-merge stacked branches). Merge or push to main only on an explicit request in that session.

1. Branch off `origin/main`; one focused change per PR; Conventional Commit subject; a `## [Unreleased]` CHANGELOG entry.
2. Before merging: sync the branch with `origin/main`, run the fast suite, push, wait for every CI check.
3. Merge, then bump on a main worktree with the bump-version skill (micro for fixes and warnings, minor for features
   or new flags/API; owner policy: never major while in 0.x), commit exactly `bump version to X.Y.Z`, push to main. One bump per PR.
4. **CHANGELOG after a bump:** syncing a branch whose `[Unreleased]` shares lines with the section the bump renamed
   can merge *without conflict* and file the branch's entry under the released version. After every sync check that
   `git diff origin/main -- CHANGELOG.md` adds only a new `## [Unreleased]` block; move the entry if not.
5. Stacked PR after its base squash-merges (and a bump landed on top): merge the final base tip into the stacked branch,
   check `git rev-parse <squash>^{tree}` equals the base tip's tree, `git merge -s ours <squash>`, then a normal
   `git merge origin/main` for the bump; retarget with `gh pr edit N --base main`. Verified on #50→#54/#55.
6. After the squash, `git rev-parse origin/main^{tree}` must equal the PR tip's tree: proof nothing else changed.
7. Squash-merged branches need `git branch -D`.

8. **Claude Code auto mode blocks the admin merge.** `gh pr merge --admin` is refused by the auto-mode classifier
   ("Merge Without Review") even when the owner asked for the merge in that session (2026-10-22, #64). The owner merges,
   or adds a Bash permission rule for it. Don't route around it (no direct push of the change to main).

## Verify
- `git diff --stat origin/main...HEAD` on a synced branch lists only that PR's own files.
- Don't treat a green `claude-review` as an independent review: see [[gotchas/claude-review-check-noop]].
