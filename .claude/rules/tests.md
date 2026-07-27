---
paths:
  - "tests/**/*.py"
  - "tests/TESTING.md"
---

# Test suite conventions

**Read `tests/TESTING.md` before adding or changing a test in the build, update, or export
path.** It is the safety contract — the invariants a gedih3 database must satisfy across
versions (resume without corruption, safe updates on every scope, readability of databases
built by any prior version). A test in those areas is asserting one of those invariants;
know which.

## Running

```bash
pytest tests/ -m "not integration and not slow"   # the fast suite; this is what CI gates on
pytest tests/ -m integration                      # needs NASA Earthdata credentials
ruff check src/
```

Markers are declared in `pyproject.toml`: `integration` for anything touching external
resources, `slow` for long-running cases. Mark accordingly — an unmarked test that hits
the network will fail the fast suite for everyone.

**Subprocess `PATH` gotcha.** Several tests invoke the `gh3_*` entry points as
subprocesses. If the environment's `bin/` is not on `PATH` they fail with
`FileNotFoundError: 'gh3_build'`. That is an environment problem, not a defect — do not
"fix" it by changing the test.

## Fixtures

`tests/conftest.py` owns the shared fixtures: `tmp_dir`, `sample_gdf`, `sample_ddf`,
`mini_h3_database`, `mini_extracted_dataset`, plus the builders `make_gedi_parquet`,
`make_partition_dir`, `make_build_log`. Use them rather than rolling your own temp tree or
synthetic frame — divergent fixtures are how two tests end up asserting incompatible
schemas.

Set `GH3_TEST_OUTPUT_DIR` to keep test output for inspection instead of having it cleaned
up (`persistent_test_dir`).

## The meta-guards

Three tests police the repository itself rather than its behaviour. If one fails, the fix
is almost never in the test:

- `test_dependencies.py` — walks the source AST and fails if an imported module is not
  declared in `pyproject.toml`, or if an `OPTIONAL_MODULES` import is not guarded by
  `try`/`except ImportError`.
- `test_release_recipe.py` — release metadata consistency.
- `test_agent_docs.py` — keeps `CLAUDE.md` and `.claude/**` from drifting: referenced
  paths must exist, `.claude/rules/*.md` must carry a `paths:` glob that matches something,
  the helper index must name real symbols, and no file may contain a hand-maintained count.

## Prefer a guard over a doc note

Anything countable — a list of entry points, a number of modules, a set of registered
names — should be asserted by a test against the source of truth, not written down in
prose. Every stale claim this repo has accumulated in agent-facing docs was a
hand-maintained count or list.
