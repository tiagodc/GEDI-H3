---
paths:
  - "src/gedih3/doctor/**/*.py"
  - "src/gedih3/cli/gh3_doctor.py"
  - "tests/test_doctor_*.py"
---

# gh3_doctor

A registry of independent diagnoses over a database or SOC tree. Each is a check function,
optionally paired with a fix, registered by name in `runner.py`.

## Alias groups

`--check` / `--fix` accept a diagnosis name, a comma-separated list, or a group alias.
Read `ALIAS_GROUPS` in `runner.py` for current membership rather than trusting a copy —
but the shape matters and is easy to get wrong:

- **`db` is the default**, and it does **not** include the SOC or tmp-partitions
  diagnoses. Running `gh3_doctor -i /db` with no `--check` does not audit everything.
- `soc` covers the download-side tree.
- `all` is the special case that resolves to every registered diagnosis.

Ask for `soc_health` or `tmp_partitions_health` by name, or use `all`.

## Contract

- **Read-only by default.** `--fix` applies only safe remedies; corrupt files are reported,
  never deleted or rewritten. Anything destructive is out of scope for this tool.
- **Exit codes**: 0 clean, 1 findings remain, 2 errors during fix. Keep new diagnoses
  consistent with this — a finding is not an error.
- `--online` decorates the report with upstream NASA availability and emits concrete
  `gh3_download` / `gh3_build` recovery commands (`upstream.py`).
- `--report` writes the machine-readable form. Keep it stable; it is consumed by scripts.
- `--fix` drops `_bbox_index.parquet` after applying remedies, since a remedy can change
  what a partition contains. Do not skip that.
- `tmp_partitions_health --fix` calls `preclean_merge_failures` and **refuses to act while
  a `gh3_build` is live**. Any new fix that touches `tmp/` needs the same guard.

## Scaling

Every diagnosis that visits every partition ships per-partition work to dask workers
through `parallel_map`. There is **no serial fallback** — `parallel_map` raises
`GediError` when no Client is registered, so a diagnosis must not be written as though it
could degrade to a loop.

Use the O(1) `os.scandir` helpers rather than recursive globs: `partition_is_empty`,
`list_year_dirs`, `year_dir_is_empty` (`doctor/parallel.py`).

Note `doctor/parquet_ops.py` has its own column-dropping variant distinct from
`utils.parquet_append_columns` — check which one you want before importing.

## Adding a diagnosis

1. New module under `doctor/diagnoses/`, decorated with `@register(name, description,
   scope=..., fix=...)`. `scope` is `'global'` or `'partition'`.
2. Add it to the right `ALIAS_GROUPS` entry — a diagnosis in no group only runs when named
   explicitly or via `all`.
3. Export it from `doctor/diagnoses/__init__.py` so registration actually happens.
4. Add `tests/test_doctor_<name>.py`. The existing doctor tests are the pattern to follow.
