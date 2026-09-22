---
paths:
  - "src/gedih3/cli/*.py"
  - "src/gedih3/cliutils.py"
  - "src/gedih3/config.py"
  - "src/gedih3/validation.py"
---

# CLI conventions

Entry points are declared in `pyproject.toml [project.scripts]` — that list is the source
of truth, and adding one there also means adding it to the conda-forge feedstock's
`entry_points`, which nothing in this repo's CI checks.

`--help` is the contract for flags. Do not restate a tool's flags in prose anywhere; the
per-tool reference is `docs/user-guide/cli-reference.md`.

## Reuse the shared builders

Check `cliutils.py` before writing anything in a CLI module:

```python
from gedih3.cliutils import (
    add_dask_args, add_verbosity_args, add_product_args, add_storage_args,  # arg builders
    setup_logging, print_banner, print_success, configure_database_path,    # setup
    cli_exception_handler,                                                  # error handling
    resolve_path_args, parse_dask_args, parse_egi_levels,                   # parsing
    load_data_from_source, get_numeric_columns, h3_col_name, progress_iter, # data
    is_internal_column, filter_data_columns, get_rasterizable_columns,      # columns
)
```

- `parse_egi_levels` turns `-egi 6` into `(6, 12)` and `-egi 6:10` into `(6, 10)`.
- `add_storage_args` supplies the whole remote-credential group
  (`--s3-endpoint`, `--s3-key`, `--s3-secret`, `--s3-profile`, `--s3-anon`,
  `--remote-user`). `setup_storage` calls `endpoint_from_s3_urls` to lift an endpoint out
  of `s3://host:port/bucket/...` arguments; an explicit `--s3-endpoint` still wins. The
  Python-API half is `resolve_s3_source` in `utils.py`.
- `setup_logging` also suppresses the noisy Dask warnings at INFO/ERROR level.
- **Internal column patterns**, auto-filtered from user-facing output: `h3_XX`, `egiXX`,
  `_egi_x`, `_egi_y`, `shot_number*`.

## Structure

Keep CLI modules thin: parse, validate, delegate. Analysis logic belongs in the library
modules so the Python API and the CLI cannot diverge.

Validate at the entry point, not deep in a worker — a multi-hour job that dies on a typo'd
variable name in hour three is the failure mode this rule exists to prevent (see
`explicit_vars_missing_in_sample`).

Wrap `main()` with `cli_exception_handler` so a `GediError` subclass becomes a readable
message and a stable exit code rather than a traceback.

## Configuration precedence

CLI args > environment variables > `~/.gedih3.env` > `config.py` defaults.
`configure_database_path` applies it; `docs/getting-started/configuration.md` documents
the user-facing variables. Do not read `os.environ` directly in a CLI.

## Output

`--help` output must stay ASCII — non-ASCII characters break on a Windows console.
Single-file outputs route through `AtomicFileWriter`; geo-vector formats
(geojson/gpkg/shp) are the deliberate exception, since they depend on extension-based
driver inference and shapefiles emit sidecars.
