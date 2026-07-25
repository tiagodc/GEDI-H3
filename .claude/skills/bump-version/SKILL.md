---
name: bump-version
description: Bump the gedih3 package version (major, minor, or micro) — update every hardcoded version location, write the CHANGELOG entry, and commit. Versioning only, by design; nothing is pushed, tagged, or published. Use /ship afterwards to release.
disable-model-invocation: true
argument-hint: "major | minor | micro"
allowed-tools: Read, Edit, Write, Bash, Grep, Glob
---

# Bump Version

Bump the gedih3 package version. The requested bump level is: **$ARGUMENTS**

## Scope: versioning is not shipping

This skill performs exactly one state change: every version touch point in the
repo is updated consistently and committed. It never pushes, never tags, never
publishes, and never talks to PyPI or conda-forge. The irreversible half of a
release — CI gating, tagging, the PyPI publish, and the conda-forge feedstock
cycle — lives in **/ship**, which independently re-verifies everything this
skill did before acting. Keeping the two apart means a bump can sit unreleased
on a branch or on main for as long as review takes, and shipping is always a
deliberate second decision.

## Step 1: Parse the argument and read the current version

The argument must be exactly one of `major`, `minor`, or `micro`. Otherwise
stop with:

> Usage: `/bump-version <major|minor|micro>`

Read `pyproject.toml`, extract `version = "X.Y.Z"`, and compute the new
version:
- `major` → (MAJOR+1).0.0
- `minor` → MAJOR.(MINOR+1).0
- `micro` → MAJOR.MINOR.(MICRO+1)

Print for the user: `Version: X.Y.Z → A.B.C`

## Step 2: Analyze changes since the last bump

Find the last version-bump commit:

```bash
git log --all --oneline --grep="bump version to" --format="%H" -1
```

If found, review `git log --oneline <hash>..HEAD`; otherwise use the last 50
commits. Categorize into **Added / Changed / Fixed / Removed** (only categories
with entries), one concise line each.

### Contributors

```bash
git log <hash>..HEAD --format='%an <%ae>' | sort -u
```

Treat the same person under different machine emails as one contributor. This
per-release set feeds only one place: the optional `### Contributors` line in
the CHANGELOG entry (skip it for solo cycles). **Never edit static
package-authorship metadata** — `src/gedih3/__init__.py:__author__`,
`docs/conf.py:author`, and `CITATION.cff:authors` are project-level credits
that persist across releases.

### Level-vs-scope check

- **micro**: bug fixes, docs, CI, dependency updates, minor tweaks only
- **minor**: new features, new CLI tools/flags, non-breaking API additions
- **major**: breaking API changes, removed/renamed public functions or CLI
  tools, major restructuring

If the commit contents suggest a different level, warn the user with a clear
explanation and confirm via AskUserQuestion before proceeding — unless the
user already stated the level deliberately in their request, which counts as
the confirmation (note the mismatch in your summary regardless).

## Step 3: Update every version touch point

Read each file first, then Edit only the version/date strings shown:

1. **`pyproject.toml`**: `version = "OLD"` → `version = "NEW"`
2. **`src/gedih3/__init__.py`**: `__version__ = "OLD"` → `"NEW"`
3. **`docs/conf.py`**: `release = "OLD"` → `"NEW"`
4. **`CITATION.cff`**: `version: OLD` → `version: NEW`, and `date-released`
   to today (YYYY-MM-DD). Leave `authors:` unchanged.
5. **`tests/test_merge_build_logs.py`**: `'package_version': 'OLD'` → `'NEW'`
6. **`recipe/meta.yaml`** — only if it exists. The in-repo recipe was retired
   after the conda-forge feedstock was created (the feedstock is the source
   of truth; its version/sha256 arrive via the autotick bot and are handled
   by /ship). If a recipe ever returns to the repo, update its
   `{% set version %}` and set `sha256: PENDING_PYPI_UPLOAD` — never compute
   a hash locally: sdists are not byte-reproducible across machines, so the
   only trustworthy digest is the one on PyPI after the real upload.

## Step 4: Verify

```bash
grep -rn "OLD_VERSION" pyproject.toml src/gedih3/__init__.py docs/conf.py \
  CITATION.cff tests/test_merge_build_logs.py
```

Any match must be fixed (matches in `CHANGELOG.md` are historical and must NOT
be modified; dependency pins that merely resemble the version, e.g.
`rioxarray >= 0.15.0`, are not version refs). Then run the guards:

```bash
pytest tests/test_release_recipe.py tests/test_dependencies.py \
  tests/test_merge_build_logs.py -m "not integration" -q
```

## Step 5: Update CHANGELOG.md

Rename the current `## [Unreleased]` heading to `## [NEW_VERSION] - YYYY-MM-DD`
(or insert a new section before the first `## [` line if no Unreleased section
exists), keeping only categories with entries. Append the optional
`### Contributors` line per Step 2.

## Step 6: Commit

```bash
git rev-parse --short HEAD   # record the pre-bump commit
git add pyproject.toml src/gedih3/__init__.py docs/conf.py CITATION.cff \
  tests/test_merge_build_logs.py CHANGELOG.md
git commit -m "bump version to NEW_VERSION"
```

The message must be exactly `bump version to X.Y.Z`. No `Co-Authored-By`
trailer.

## Step 7: Hand off

Print and stop — nothing is pushed:

```
Version bump complete: OLD → NEW
Pre-bump commit:  <short hash>
Bump commit:      <short hash>

This was versioning only. To release:
  - get the bump commit onto main (merge the PR / push), then
  - run /ship — it re-verifies the bump, gates on green CI, tags vNEW to
    trigger the PyPI publish, and drives the conda-forge feedstock cycle
    to completion under a background monitor.
```
