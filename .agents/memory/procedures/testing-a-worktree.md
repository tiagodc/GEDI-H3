# Testing a worktree
> run the suite against a worktree: PYTHONPATH=src, env bin on PATH, known order-dependent test
updated: 2026-10-20T11:45:23-04:00

PR branches are often checked out as sibling git worktrees. To test one without reinstalling the package:

1. Use the dev conda env's interpreter (`gedih3` env from `environment.yml`, or a contributor's own dev env).
2. Run from the worktree root with `PYTHONPATH=src <env>/bin/python -m pytest tests/ -m "not integration and not slow"`.
3. Put `<env>/bin` on `PATH` too: `tests/test_cli_pipeline.py` spawns the console scripts via subprocess and fails with
   `FileNotFoundError` without them.
4. On a low-core host the fast suite takes minutes; run it in the background and wait once.
5. Lint: `ruff check src/` (120 cols).

## Verify
- `tests/test_merge_resume.py::test_empty_year_dir_is_skipped` fails when run alone (also on main) but passes in the full
  fast suite and in CI: order-dependent, not a regression. Seen 2026-09-29.
- `gh` may not be on PATH on shared hosts; check the dev env's `bin/`. Auth via `GITHUB_TOKEN`.
