# Testing a worktree
> run the suite against a worktree: PYTHONPATH=src, env bin on PATH, known order-dependent test
updated: 2026-10-26T11:12:31-04:00

PR branches are often checked out as sibling git worktrees. To test one without reinstalling the package:

1. Use the dev conda env's interpreter (`gedih3` env from `environment.yml`, or a contributor's own dev env).
2. Run from the worktree root with `PYTHONPATH=src <env>/bin/python -m pytest tests/ -m "not integration and not slow"`.
3. Put `<env>/bin` on `PATH` too: `tests/test_cli_pipeline.py` spawns the console scripts via subprocess and fails with
   `FileNotFoundError` without them.
4. The fast suite takes ~5-7 min; run it in the background and wait once. `nproc` can report 1 when the shell sets
   `OMP_NUM_THREADS=1`: check cores with `lscpu`; suites of several worktrees can run side by side.
5. Lint: `ruff check src/` (120 cols).

6. End-to-end `gh3_build` tests need no network: tests write synthetic GEDI HDF5 granules and run the CLI as a
   subprocess (`tests/test_phased_product_updates.py`, `test_phased_updates_through_the_cli`). Pass a region that starts
   with `-` as `--region=-55,0,-50,1`, or argparse reads it as a flag. A "kill" is simulated by saving the build log
   with `H3BuildLogger(...).save_log('<STATUS>')` before the next CLI run.

## Verify
- `tests/test_merge_resume.py` `test_empty_year_dir_is_skipped` fails when run alone (also on main) but passes in the full
  fast suite and in CI: order-dependent, not a regression.
- `gh` may not be on PATH on shared hosts; check the dev env's `bin/`. Auth via `GITHUB_TOKEN`.
