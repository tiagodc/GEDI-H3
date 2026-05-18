---
name: bump-version
description: Bump the gedih3 package version (major, minor, or micro), update all hardcoded version locations, update CHANGELOG.md with a summary of changes, and commit.
disable-model-invocation: true
argument-hint: "major | minor | micro"
allowed-tools: Read, Edit, Write, Bash, Grep, Glob
---

# Bump Version

Bump the gedih3 package version. The requested bump level is: **$ARGUMENTS**

## Step 0: Validate input

If `$ARGUMENTS` is not exactly one of `major`, `minor`, or `micro`, stop immediately and tell the user:

> Usage: `/bump-version <major|minor|micro>`

## Step 1: Read current version

Read `pyproject.toml` and extract the current version from the line matching `version = "X.Y.Z"`.
Parse it into three integers: MAJOR, MINOR, MICRO.

Compute the new version based on `$ARGUMENTS`:
- `major` → (MAJOR+1).0.0
- `minor` → MAJOR.(MINOR+1).0
- `micro` → MAJOR.MINOR.(MICRO+1)

Print for the user: `Version: X.Y.Z → A.B.C`

## Step 2: Analyze changes since last version bump

Find the last version-bump commit:

```bash
git log --all --oneline --grep="bump version to" --format="%H" -1
```

If a bump commit is found, get the log since then:

```bash
git log --oneline <hash>..HEAD
```

If no bump commit exists, use the full log (last 50 commits max):

```bash
git log --oneline -50
```

Review the commits and categorize them into: **Added**, **Changed**, **Fixed**, **Removed**. Keep descriptions concise (one line each). Only include categories that have entries.

### Determine actual contributors

In parallel with the categorization, identify the distinct git authors of the commits in this bump cycle:

```bash
git log <hash>..HEAD --format='%an <%ae>' | sort -u
```

(If there is no prior bump commit, run the same command with the same range fallback used above.) This list is the **per-release contributor set** and is used downstream in two places:
- The optional `### Contributors` line at the bottom of the CHANGELOG entry (see Step 5).
- The commit-message guard in Step 6.

**Do NOT use this list to edit static package-authorship metadata.** `src/gedih3/__init__.py:__author__`, `docs/conf.py:author`, and `CITATION.cff:authors` enumerate the project's overall authors — they are package-level credits that persist across releases and should remain untouched by this skill regardless of who happened to commit in this cycle.

Then evaluate whether the requested bump level (`$ARGUMENTS`) matches the scope of changes:
- **micro**: bug fixes, documentation, CI, dependency updates, minor tweaks only
- **minor**: new features, new CLI tools/flags, non-breaking API additions
- **major**: breaking API changes, removed public functions/classes, renamed CLI tools, major restructuring

If the changes suggest a different level would be more appropriate, **warn the user** with a clear explanation of why and ask for confirmation using AskUserQuestion. Do NOT proceed without explicit approval.

## Step 3: Update version in all hardcoded locations

Read each file first, then use the Edit tool to replace the OLD version with the NEW version in exactly these files. **Edit only the version/date strings shown.** Do not modify any other field in these files — in particular, do not edit `__author__`, `author`, or `authors:` blocks. Those are persistent package-level credits and are out of scope for the bump skill (see Step 2's contributor-set guard).

1. **`pyproject.toml`**: `version = "OLD"` → `version = "NEW"`
2. **`src/gedih3/__init__.py`**: `__version__ = "OLD"` → `__version__ = "NEW"`
3. **`docs/conf.py`**: `release = "OLD"` → `release = "NEW"`
4. **`CITATION.cff`**: `version: OLD` → `version: NEW` — also update `date-released` to today's date (YYYY-MM-DD). Leave the `authors:` block unchanged.
5. **`tests/test_merge_build_logs.py`**: `'package_version': 'OLD'` → `'package_version': 'NEW'`
6. **`recipe/meta.yaml`**: `{% set version = "OLD" %}` → `{% set version = "NEW" %}`

## Step 4: Verify updates

Run this command (substituting the actual old version string):

```bash
grep -rn "OLD_VERSION" pyproject.toml src/gedih3/__init__.py docs/conf.py CITATION.cff tests/test_merge_build_logs.py recipe/meta.yaml
```

If any matches are found, fix them before proceeding. Matches in CHANGELOG.md are expected (historical entries) and should NOT be modified.

> **Note**: `recipe/meta.yaml` also has a commented-out `sha256` field that must be updated **manually** after uploading to PyPI. The skill only updates the version string.

## Step 5: Update CHANGELOG.md

Read `CHANGELOG.md`. Insert a new section immediately before the first existing `## [` line.

Format:

```
## [NEW_VERSION] - YYYY-MM-DD

### Added
- item

### Changed
- item

### Fixed
- item
```

Only include categories (Added/Changed/Fixed/Removed) that have actual entries from the git log analysis in Step 2. Do not include empty categories.

### Optional: per-release contributors line

If the contributor set from Step 2 has more than one distinct author, append a trailing `### Contributors` line after the last category, listing the names from the contributor set (display names only, no emails). Skip this section entirely when only one author contributed — solo bump cycles get no contributors line.

This is the *only* place per-release contributor info is recorded. Do not add Co-Authored-By trailers to the bump commit (Step 6) and do not edit the static package-authorship metadata (Step 3).

## Step 6: Stage and commit

Record the current HEAD before committing:

```bash
git rev-parse --short HEAD
```

Stage all modified files and commit:

```bash
git add pyproject.toml src/gedih3/__init__.py docs/conf.py CITATION.cff tests/test_merge_build_logs.py CHANGELOG.md recipe/meta.yaml
git commit -m "bump version to NEW_VERSION"
```

The commit message must be exactly `bump version to X.Y.Z` with the actual new version. Do NOT add a Co-Authored-By trailer — the per-release contributor set lives in the CHANGELOG entry's optional `### Contributors` line (Step 5), not on the bump commit. Static package-authorship credits (`__author__`, `docs/conf.py:author`, `CITATION.cff:authors`) are intentionally not touched by this skill — they enumerate the project's overall authors and persist across releases regardless of who happened to commit in this cycle.

## Step 7: Print summary

Display:

```
Version bump complete: OLD → NEW
Pre-bump commit:  <short hash>
Bump commit:      <short hash>
Files updated:    7
Run `git push` when ready to publish.
```
