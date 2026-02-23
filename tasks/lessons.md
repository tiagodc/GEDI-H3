# Lessons Learned

## 2026-02-22: load_h5 returns shot_number as index, not column

**Failure mode**: `_add_variables_to_partition` called `pd.concat(frames, ignore_index=True)` which discarded the `shot_number` index from `load_h5()`, then `drop_duplicates(subset=['shot_number'])` raised `KeyError`.

**Detection signal**: `KeyError(Index(['shot_number'], dtype='str'))` in Dask worker logs during variable-only update.

**Prevention rule**: When using `load_h5()` output, always remember it returns a DataFrame **indexed by shot_number**. Use `reset_index()` before operations that expect `shot_number` as a regular column. Never use `ignore_index=True` in concat if you need the shot_number.

## 2026-02-22: os.path.basename on glob paths ending with '/'

**Failure mode**: `glob.glob('path/h3_*/')` returns paths ending with `/`. `os.path.basename('/path/h3_03=abc/')` returns `''`, making log messages unhelpful.

**Prevention rule**: Always `rstrip('/')` before `os.path.basename()` on glob results.

## 2026-02-22: parquet_merge_files was not atomic

**Failure mode**: `ParquetWriter(ofile, ...)` writes directly to the final path. If killed mid-write, the output file is corrupt and unrecoverable.

**Fix applied**: Write to `ofile + '.merge.tmp'`, atomically rename via `os.replace()` after `writer.close()`. Clean stale temps on entry.

## 2026-02-22: `_resuming` flag skipped filter merging, breaking variable-only update resume

**Failure mode**: `load_log_data()` set `_resuming=True` for FAILED/INTERRUPTED logs. `H3BuildLogger.__init__()` skipped ALL filter merging on resume, leaving `new_product_vars=None`. The CLI couldn't detect "variable-only update" vs "full build," fell through to the full build path, and re-downloaded all products from S3 (L2A + L4A + L4C instead of just L4C).

**Detection signal**: "Subsetting GEDI02_A..." log messages for L2A/L4A files when only L4C was expected. All products being re-downloaded on resume.

**Prevention rule**: Never skip filter merging based on log status alone. Always merge CLI args against the existing log to determine what (if anything) is new. Use `is_up_to_date()` to short-circuit when nothing has changed. The `_resuming` flag concept is fundamentally wrong — resume semantics should be derived from comparing requested vs logged state, not from a blanket "skip everything" flag.
