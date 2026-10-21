# Project index
<!-- protocol-version: 2 · mode: shared · rules: .agents/memory/PROTOCOL.md -->

## Summary
gedih3: Python library + CLI turning NASA GEDI lidar HDF5 granules into H3/EGI-indexed parquet databases, queryable and rasterizable.
Delivers via PyPI and the conda-forge feedstock; mature 0.x, small focused fix/feature PRs onto a review-protected main.
Invariants, architecture and design priorities live in `CLAUDE.md`; this memory holds process facts and past traps.

## Routes
- any task → `CLAUDE.md` (Invariants, Design priorities, "Where the details live" table)
- writing a helper → find-existing-helper skill, `.claude/skills/find-existing-helper/SKILL.md`
- build / merge / resume / update → `.claude/rules/build-pipeline.md`, `src/gedih3/gh3builder.py`, `src/gedih3/logger.py`, [[concepts/build-log-resume-state]]
- query / load / pushdown → `.claude/rules/query-engine.md`, `src/gedih3/gh3driver.py`, `src/gedih3/sqlutils.py`
- H3/EGI math, rasters → `.claude/rules/spatial-indexing.md`, `src/gedih3/h3utils.py`, `src/gedih3/egi/`, `src/gedih3/raster/`
- CLI tools → `.claude/rules/cli.md`, `src/gedih3/cli/`, entry points in `pyproject.toml [project.scripts]`
- doctor diagnoses → `.claude/rules/doctor.md`, `src/gedih3/doctor/`
- tests → `tests/TESTING.md`, `.claude/rules/tests.md`, [[procedures/testing-a-worktree]]
- editing CLAUDE.md or `.claude/**` → `tests/test_agent_docs.py` (no counts, no machine paths, size caps)
- opening / landing a PR → [[procedures/landing-prs-on-main]]
- CI checks → `.github/workflows/`, monitor-ci skill; review bot → [[gotchas/claude-review-check-noop]]
- release → bump-version then ship skills in `.claude/skills/`, `CHANGELOG.md`
- user docs → `docs/` (Sphinx, `docs/conf.py`), `README.md`

## Memory
- [[concepts/build-log-resume-state]] — how gh3_build resumes an interrupted update: _pending_update, _pending_variable_update, update_ran
- [[gotchas/claude-review-check-noop]] — claude-review CI check passed without reviewing until #43; how to test workflow changes
- [[procedures/landing-prs-on-main]] — main is review-protected: owner admin-squash-merges; agents open PRs, never merge unasked
- [[procedures/testing-a-worktree]] — run the suite against a worktree: PYTHONPATH=src, env bin on PATH, known order-dependent test
