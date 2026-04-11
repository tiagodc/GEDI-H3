---
name: monitor-ci
description: Monitor GitHub Actions CI runs for the latest push, wait for all workflows to complete, diagnose any failures or warnings, fix root causes in source, and push the fix commit.
disable-model-invocation: true
allowed-tools: Read, Edit, Write, Bash, Grep, Glob
---

# Monitor CI

Monitor GitHub Actions for the most recent push, wait for completion, and fix any failures or warnings found.

**Always use the gh3_dev conda environment:**
- `GH=/gpfs/data1/vclgp/decontot/environments/gh3_dev/bin/gh`

---

## Step 1: Identify the commit being monitored

Get the current HEAD SHA to know which run to watch:

```bash
git rev-parse HEAD
```

---

## Step 2: Wait for all workflows to complete

Poll every 30 seconds until every run triggered by HEAD is no longer `in_progress` or `queued`.

```bash
GH=/gpfs/data1/vclgp/decontot/environments/gh3_dev/bin/gh
$GH run list --repo tiagodc/GEDI-H3 --commit $(git rev-parse HEAD) --json databaseId,name,status,conclusion
```

While any run has `status != "completed"`, print a one-line status summary and wait. Once all are complete, proceed.

---

## Step 3: Report results

Print a table of all runs for this commit:

```
Workflow            Status      Conclusion
─────────────────── ─────────── ───────────
Build               completed   success
Tests               completed   success
Lint                completed   failure   ← needs fix
Deploy Docs         completed   success
```

---

## Step 4: For each failed or warning run, collect the full log

```bash
$GH run view <run_id> --repo tiagodc/GEDI-H3 --log-failed 2>&1
```

Parse the output to extract:
- **Errors**: lines containing `##[error]` — these are lint/build/test failures
- **Warnings**: lines containing `warning:` — these may indicate misconfiguration

Collect all unique error messages and warnings. Display them to the user.

---

## Step 5: Diagnose and fix root causes

For each error or warning:

1. **Read the relevant source file** before making any changes.
2. Identify the root cause (don't patch symptoms — fix the underlying problem).
3. Apply the minimal fix using the Edit tool.

Common failure patterns:

| Symptom | Likely cause | Fix |
|---|---|---|
| `F823 Local variable referenced before assignment` | A name is imported both at module level and inside a function (inline import shadows the module-level one) | Move the import to module level; remove the duplicate inline import |
| `Invalid # noqa directive` | `# noqa: plain text` instead of `# noqa: CODE` | Replace with the correct ruff code (e.g. `B018`, `F401`) |
| `F401 imported but unused` | Import exists only for side-effects or availability check | Add `# noqa: F401` |
| `B018 useless expression` | Expression statement with no side-effects (e.g. `obj.attr`) | Add `# noqa: B018` or rewrite as an assignment |
| Test failure | Logic error, missing fixture, changed API | Read test + source, fix the root cause |
| Docs build failure | Missing reference, broken cross-link, syntax error in RST/MD | Read the failing doc file and fix |

After applying all fixes, verify locally where possible (e.g. `python -c "import gedih3"` to catch import errors).

---

## Step 6: Commit and push the fix

Stage only the files you changed:

```bash
git add <files>
git commit -m "fix ci: <concise description of what was wrong>"
git push
```

The commit message must start with `fix ci:` and describe the root cause, not the symptom.

---

## Step 7: Monitor the fix run

Repeat Steps 2–4 for the new HEAD commit. If new failures appear, loop back to Step 5.

Once all workflows show `conclusion: success` with no warnings, print:

```
All CI checks passed. ✓
  Build      success
  Tests      success
  Lint       success
  Deploy Docs success
```

---

## Notes

- The `pages build and deployment` workflow is triggered by GitHub Pages internally and cannot be influenced directly — ignore it unless it fails.
- Ruff is not installed in the gh3_dev environment locally; lint errors must be inferred from the CI log and fixed by reading/editing source directly.
- Do **not** add `# noqa` blanket suppressions — always use the specific rule code.
- Do **not** use `--no-verify` or skip hooks.
