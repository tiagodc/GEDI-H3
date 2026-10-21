# Build-log resume state
> how gh3_build resumes an interrupted update: _pending_update, _pending_variable_update, update_ran
updated: 2026-10-20T22:59:21-04:00

The first `save_log` of a `gh3_build` run that goes on to extract (status `PARTITIONING`) already stores the *merged* request: the union region,
the union dates, and the new products/variables (`to_dict` also writes them into `log_data['products']` in place). A
resume that only read those fields would see no update at all. Three markers keep the interrupted run's intent:

- `_pending_update` (`H3BuildLogger.to_dict`, restored by `_restore_pending_update` in `__init__`): the added area,
  added dates and new product variables. Restored into `new_spatial` / `new_temporal` / `new_product_vars`, so every
  getter (`get_spatial`, `get_finished_granules`, `stage1_full_scope`, `is_up_to_date`) and the Stage 1 scope
  fingerprint answer as in the interrupted run. Written on every save until `COMPLETED`, `update_ran`, or Stage 2 start.
- `_pending_variable_update` (set by `cli/gh3_build.py` when Stage 2 starts): the variables Stage 2 must still add.
  While present the marker above is not written, and the resume skips Stage 1. Issue #56: the doctor clears it.
- `update_ran` (set by the CLI before its final saves): a final `MERGING` save after failed merges drops the marker,
  so the retry only re-merges.

Related flags:
- `requested_expansion` is what *this* run asks to widen, computed before the restore. It vetoes the merge-only
  shortcut (`_detect_merge_resume_signal(has_pending_new_work=...)`); a restored expansion followed a drained Stage 1.
- `new_spatial` is the area a request adds (union minus stored). A narrower or identical region sets none (plain update).
- A spatial-only expansion reads only granules finished under the stored region (`stage1_only_granules` →
  `build_h3db(only_granules=)`); others stay `PENDING` for the next build, their leftover tmp fragments dropped
  (`_drop_deferred_fragments`).

Tests that pin this: `tests/test_logger_resume_skip.py`, `tests/test_resume_variable_update.py`,
`tests/test_spatial_expansion.py` (end-to-end through the CLI on synthetic HDF5).
