# Variable adds: what one run covers and what it reads
> a var add with no -r/-t skips Stage 1; it used to read a product's whole stored var list (fixed by #77)
updated: 2026-11-11T16:42:51-05:00

**Status:** 2 fixed by #77 (fix/variable-add-new-vars-only); 1 is by design.

## Symptom
1. `gh3_build -l4a <new vars>` on a database whose SOC also holds new granules adds the variables to every indexed
   granule but does not index the new granules in that run.
2. Adding a few variables to a product the database already holds cost about as much as adding the whole product:
   every stored variable of it was read from every granule and staged as fragments (scratch, merge memory, #70).

## Cause
1. `is_variable_only_update` (`src/gedih3/cli/gh3_build.py`): new variables and no new region/dates → Stage 1 is
   skipped. A mixed update (Phase 1 + Phase 2) needs a spatial or temporal widening too.
2. `merge_product_vars` (`src/gedih3/logger.py`) returned the whole merged list of any product that gained a name;
   `_var_fan_granule` reads `['shot_number'] + var_list`, and `parquet_join_columns` drops present columns only at merge.

## Fix
1. Run the variable add and the granule update as two runs. Add the variables first, so the later (phased) run writes
   them for the new granules too. Bare product flags (`-l2a`) mean "all variables": on a database storing a subset,
   that is a database-wide variable add.
2. #77: `merge_product_vars` returns only the added names (whole list for a new product, `None` for "all").

## Evidence
#77: unit + `test_variable_add_reads_only_the_new_variables` (spy on `load_h5`) fail on main, pass on the branch.
Real data (4 V003 granules, 3 L4A vars added): identical to main and to a fresh build; about half the wall time and
about a sixth of the peak scratch of main; SIGKILL at fan-out, merge and metadata merge resumes identical.
