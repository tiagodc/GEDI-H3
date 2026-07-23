---
name: bump-version
description: Bump the gedih3 package version (major, minor, or micro), update all hardcoded version locations, update CHANGELOG.md with a summary of changes, and commit. Also finalizes a release by pinning the conda recipe's sha256 once the sdist is on PyPI. Pass an optional `ship` argument to run the whole release unattended — push, gate on green CI, tag, wait for PyPI, and pin the hash — in one invocation.
disable-model-invocation: true
argument-hint: "major | minor | micro [ship]"
allowed-tools: Read, Edit, Write, Bash, Grep, Glob
---

# Bump Version

Bump the gedih3 package version. The requested bump level is: **$ARGUMENTS**

## Why this skill has two phases

A release cannot be completed in one pass, because of one hard constraint:

**The conda recipe's `sha256` cannot be known until the sdist is on PyPI.**

The recipe does not build from git — it downloads the published sdist and
verifies it against a pinned digest. sdists are *not* byte-reproducible across
machines: building the same commit locally and in CI produces different
tarballs, so a locally computed hash is worthless. The digest only exists once
the release workflow has uploaded the real file.

That forces this order:

```
  Phase A  bump versions + CHANGELOG, invalidate the recipe hash, commit
     ↓     (you push, then tag → the Release workflow publishes to PyPI)
  Phase B  pin the real sha256, verify it against PyPI, commit
     ↓     (conda-forge)
```

Between the two phases the recipe carries the literal marker
`PENDING_PYPI_UPLOAD` instead of a digest. That is deliberate: it is a
*declared* not-ready state, so a hash left over from the previous release can
never be mistaken for a current one. `tests/test_release_recipe.py` enforces
this — the offline guards stay green while the marker is present, and the
integration guard verifies the digest against PyPI's real bytes once it is not.

The `ship` argument does not remove the two phases — the PyPI-upload gap is
unavoidable — it *automates across* them: it performs Phase A, pushes, waits
for the artifact to land on PyPI, then performs Phase B, all in one invocation.
The wait is real (a few minutes); `ship` just polls through it so you issue one
command instead of three. The only thing it will not do automatically is tag a
commit whose CI is not fully green.

## Step 0: Decide which phase to run

Run this first:

```bash
PKG=$(python -c "import tomllib; print(tomllib.load(open('pyproject.toml','rb'))['project']['version'])")
SHA=$(grep -oP '^\s*sha256:\s*\K\S+' recipe/meta.yaml)
PUBLISHED=$(curl -s https://pypi.org/pypi/gedih3/json | python -c "import json,sys; print(json.load(sys.stdin)['info']['version'])" 2>/dev/null)
echo "pyproject=$PKG  recipe_sha=$SHA  pypi_latest=$PUBLISHED"
```

- **`sha256` is `PENDING_PYPI_UPLOAD`** → a bump is already in flight. Skip to
  **Phase B** to finish it — even if the user passed `ship`, since you cannot
  start a new release while one is half-done. If `$PKG` is not yet on PyPI, stop
  and tell the user to push the tag first (Step B0 explains how).
- **Otherwise** → run **Phase A** below (then Ship mode, if `ship` was given).

### Parse the arguments

The first token is the bump level; an optional second token `ship` requests
the fully automated end-to-end release.

- Level must be exactly one of `major`, `minor`, or `micro`. Otherwise stop:
  > Usage: `/bump-version <major|minor|micro> [ship]`
- If the second token is present it must be exactly `ship`; anything else →
  same usage error.

**Without `ship`** (default): run Phase A, then stop and print the manual
commands (Step A8). Nothing is pushed or published. This is the safe default
and is unchanged.

**With `ship`**: the user has consented to an unattended release by typing
`ship` — no further confirmation is asked. Run Phase A, then **Ship mode**
below, which pushes, gates on CI, tags, waits for PyPI, and pins the hash in
one go. The one hard guardrail: it must **not** create the tag unless CI on the
bump commit is fully green (see Ship Step S2).

---

# PHASE A — bump

## Step A1: Read current version

Read `pyproject.toml` and extract the current version from the line matching
`version = "X.Y.Z"`. Parse it into three integers: MAJOR, MINOR, MICRO.

Compute the new version based on `$ARGUMENTS`:
- `major` → (MAJOR+1).0.0
- `minor` → MAJOR.(MINOR+1).0
- `micro` → MAJOR.MINOR.(MICRO+1)

Print for the user: `Version: X.Y.Z → A.B.C`

## Step A2: Analyze changes since last version bump

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

Review the commits and categorize them into: **Added**, **Changed**, **Fixed**,
**Removed**. Keep descriptions concise (one line each). Only include categories
that have entries.

### Determine actual contributors

In parallel with the categorization, identify the distinct git authors of the
commits in this bump cycle:

```bash
git log <hash>..HEAD --format='%an <%ae>' | sort -u
```

(If there is no prior bump commit, run the same command with the same range
fallback used above.) This list is the **per-release contributor set** and is
used downstream in two places:
- The optional `### Contributors` line at the bottom of the CHANGELOG entry.
- The commit-message guard in Step A6.

**Do NOT use this list to edit static package-authorship metadata.**
`src/gedih3/__init__.py:__author__`, `docs/conf.py:author`, and
`CITATION.cff:authors` enumerate the project's overall authors — they are
package-level credits that persist across releases and should remain untouched
by this skill regardless of who happened to commit in this cycle.

Then evaluate whether the requested bump level (`$ARGUMENTS`) matches the scope:
- **micro**: bug fixes, documentation, CI, dependency updates, minor tweaks only
- **minor**: new features, new CLI tools/flags, non-breaking API additions
- **major**: breaking API changes, removed public functions/classes, renamed CLI
  tools, major restructuring

If the changes suggest a different level would be more appropriate, **warn the
user** with a clear explanation of why and ask for confirmation using
AskUserQuestion. Do NOT proceed without explicit approval.

## Step A3: Update version in all hardcoded locations

Read each file first, then use the Edit tool to replace the OLD version with the
NEW version in exactly these files. **Edit only the version/date strings shown.**
Do not modify any other field — in particular, do not edit `__author__`,
`author`, or `authors:` blocks.

1. **`pyproject.toml`**: `version = "OLD"` → `version = "NEW"`
2. **`src/gedih3/__init__.py`**: `__version__ = "OLD"` → `__version__ = "NEW"`
3. **`docs/conf.py`**: `release = "OLD"` → `release = "NEW"`
4. **`CITATION.cff`**: `version: OLD` → `version: NEW` — also update
   `date-released` to today's date (YYYY-MM-DD). Leave `authors:` unchanged.
5. **`tests/test_merge_build_logs.py`**: `'package_version': 'OLD'` → `'NEW'`
6. **`recipe/meta.yaml`**: `{% set version = "OLD" %}` → `{% set version = "NEW" %}`

## Step A4: Invalidate the conda recipe hash

**This is the step that prevents a stale hash shipping.** The recipe now
declares the new version, but its `sha256` still describes the *previous*
release's bytes. Replace it with the canonical pending marker:

```yaml
  sha256: PENDING_PYPI_UPLOAD
```

Do **not** attempt to compute the hash here — a locally built sdist does not
match what CI uploads. Phase B fills this in from the published artifact.

## Step A5: Verify updates

```bash
grep -rn "OLD_VERSION" pyproject.toml src/gedih3/__init__.py docs/conf.py \
  CITATION.cff tests/test_merge_build_logs.py recipe/meta.yaml
```

Any matches must be fixed before proceeding. Matches in `CHANGELOG.md` are
expected (historical entries) and must NOT be modified.

Then confirm the version is consistent everywhere and the guards agree:

```bash
pytest tests/test_release_recipe.py tests/test_dependencies.py \
  -m "not integration" -q
```

## Step A6: Update CHANGELOG.md

Insert a new section immediately before the first existing `## [` line:

```
## [NEW_VERSION] - YYYY-MM-DD

### Added
- item

### Changed
- item

### Fixed
- item
```

Only include categories that have actual entries. Do not include empty ones.

### Optional: per-release contributors line

If the contributor set has more than one distinct author, append a trailing
`### Contributors` line listing the names (display names only, no emails). Skip
it entirely for solo bump cycles. This is the *only* place per-release
contributor info is recorded — do not add `Co-Authored-By` trailers to the bump
commit, and do not edit static package-authorship metadata.

## Step A7: Stage and commit

Record the current HEAD before committing:

```bash
git rev-parse --short HEAD
```

```bash
git add pyproject.toml src/gedih3/__init__.py docs/conf.py CITATION.cff \
  tests/test_merge_build_logs.py CHANGELOG.md recipe/meta.yaml
git commit -m "bump version to NEW_VERSION"
```

The message must be exactly `bump version to X.Y.Z`. No `Co-Authored-By`
trailer.

## Step A8: hand off to release

**If `ship` was NOT given**, print this and stop — nothing is pushed:

```
Version bump complete: OLD → NEW
Pre-bump commit:  <short hash>
Bump commit:      <short hash>
Files updated:    7  (recipe sha256 invalidated → PENDING_PYPI_UPLOAD)

NEXT — publish to PyPI (this is the irreversible step):

    git push origin main
    git tag vNEW && git push origin vNEW

The Release workflow builds, runs `twine check --strict`, verifies the tag
matches pyproject.toml, and publishes via Trusted Publishing. Watch it:

    gh run watch $(gh run list --workflow Release --limit 1 --json databaseId \
      --jq '.[0].databaseId') --repo tiagodc/GEDI-H3 --exit-status

THEN — re-run `/bump-version` (any argument) to finalize the conda recipe.

Or next time run `/bump-version NEW_LEVEL ship` to do all of this unattended.
```

Do not tag or push — publishing is the user's decision.

**If `ship` WAS given**, do not print the manual instructions — continue
straight into Ship mode.

---

# SHIP MODE — automated end-to-end release

Runs only when the user passed `ship`. Typing `ship` is the consent; do not ask
for further confirmation. Everything here is unattended **except** the CI gate
in S2, which is a hard stop, not a prompt.

## Ship Step S1: push the bump commit

```bash
git push origin main
```

## Ship Step S2: CI-green gate — the one hard guardrail

**Do not create the tag unless every CI run on the bump commit concluded
`success`.** Wait for them to finish, then check:

```bash
SHA=$(git rev-parse HEAD)
# Wait until nothing is still running (cap the wait; don't loop forever).
for i in $(seq 1 40); do
  PENDING=$(gh run list --repo tiagodc/GEDI-H3 --commit "$SHA" \
    --json status --jq '[.[]|select(.status!="completed")]|length')
  [ "$PENDING" = "0" ] && [ "$(gh run list --repo tiagodc/GEDI-H3 --commit "$SHA" --json databaseId --jq 'length')" -ge 1 ] && break
  sleep 30
done
FAILED=$(gh run list --repo tiagodc/GEDI-H3 --commit "$SHA" \
  --json name,conclusion --jq '[.[]|select(.conclusion!="success")]')
echo "non-success runs: $FAILED"
```

- If `FAILED` is anything other than `[]` → **STOP. Do not tag.** Report which
  workflow failed and that the bump commit is pushed but unshipped (recoverable:
  fix, push, re-run `/bump-version LEVEL ship` — Step 0 will detect the pending
  hash and, since the tag was never made, a fresh ship proceeds from S1).
- If CI never finishes within the cap → same: stop, do not tag, report that CI
  did not complete in time.
- Only when every run is `success` → proceed to S3.

## Ship Step S3: tag and push — the irreversible line

```bash
git tag -a vNEW -m "gedih3 vNEW" && git push origin vNEW
```

The Release workflow re-checks the tag against pyproject.toml as a second
guard; if they somehow disagree it refuses to publish.

## Ship Step S4: wait for PyPI

Watch the Release workflow, then confirm PyPI is serving the new sdist:

```bash
RUN=$(gh run list --repo tiagodc/GEDI-H3 --workflow Release --event push \
  --limit 1 --json databaseId --jq '.[0].databaseId')
gh run watch "$RUN" --repo tiagodc/GEDI-H3 --exit-status --interval 20
curl -s -o /dev/null -w "%{http_code}\n" https://pypi.org/pypi/gedih3/NEW/json
```

- If the Release workflow fails → **STOP.** The tag is pushed but PyPI was not
  updated. Report the failure and where things stand: the recipe still carries
  `PENDING_PYPI_UPLOAD`, so re-running after a fix will resume at Phase B once
  the artifact exists (a failed publish may need a follow-up patch release,
  since the tag is now used). Do not fabricate a hash.
- Only when the run is `success` and the PyPI check returns `200` → proceed to
  Phase B below (S5 is just "run Phase B").

## Ship Step S5: finalize

Run **Phase B** (below) to pin the verified hash and commit, then push:

```bash
git push origin main
```

Then print the Phase B conda-forge handoff (Step B4).

---

# PHASE B — finalize the conda recipe

Run this only once `$PKG` is live on PyPI.

## Step B0: Confirm the release actually landed

```bash
VER=$(python -c "import tomllib; print(tomllib.load(open('pyproject.toml','rb'))['project']['version'])")
curl -s https://pypi.org/pypi/gedih3/$VER/json -o /dev/null -w "%{http_code}\n"
```

If this is not `200`, stop and tell the user the tag has not been pushed (or the
Release workflow has not finished), and repeat the commands from Step A8.

## Step B1: Pin the real hash

Download the published sdist and hash it locally — do not simply copy the value
from the JSON API, so that the digest is confirmed against bytes actually
served:

```bash
VER=$(python -c "import tomllib; print(tomllib.load(open('pyproject.toml','rb'))['project']['version'])")
curl -sL -o /tmp/gedih3-$VER.tar.gz \
  "https://pypi.org/packages/source/g/gedih3/gedih3-$VER.tar.gz"
LOCAL=$(sha256sum /tmp/gedih3-$VER.tar.gz | cut -d' ' -f1)
REPORTED=$(curl -s https://pypi.org/pypi/gedih3/$VER/json | python -c \
  "import json,sys; print([u['digests']['sha256'] for u in json.load(sys.stdin)['urls'] if u['packagetype']=='sdist'][0])")
echo "computed=$LOCAL"; echo "reported=$REPORTED"
```

The two must match. If they differ, **stop and report it** — that is a
supply-chain red flag, not something to work around.

Then replace `PENDING_PYPI_UPLOAD` in `recipe/meta.yaml` with the digest.

## Step B2: Verify the recipe is safe to submit

```bash
pytest tests/test_release_recipe.py tests/test_dependencies.py -q
```

This checks that the recipe version matches `pyproject.toml`, the digest is
well-formed, the source URL still interpolates `{{ version }}`, the pinned hash
matches what PyPI serves, and the recipe's dependency names *and* lower bounds
have not drifted from `pyproject.toml`.

Also confirm the recipe still renders (it is Jinja-templated, so a plain YAML
load will not do) and that the sdist carries the licence files the recipe
declares in `license_file`:

```bash
tar tzf /tmp/gedih3-$VER.tar.gz | grep -E "/(LICENSE|NOTICE)$"
```

Both must be present, or the conda build fails on `license_file` and the
licence's redistribution condition is not met.

## Step B3: Commit

```bash
git add recipe/meta.yaml
git commit -m "build(recipe): pin sha256 for NEW_VERSION"
```

## Step B4: Tell the user how to ship to conda-forge

Print this, substituting the real version and digest:

```
Recipe finalized for NEW: sha256 pinned and verified against PyPI.

    git push origin main

FIRST RELEASE ONLY — create the feedstock:

    gh repo fork conda-forge/staged-recipes --clone --remote
    cd staged-recipes && git checkout -b gedih3
    mkdir -p recipes/gedih3 && cp <repo>/recipe/meta.yaml recipes/gedih3/
    git add recipes/gedih3 && git commit -m "Add gedih3"
    git push -u origin gedih3
    gh pr create --repo conda-forge/staged-recipes \
      --title "Add gedih3" --body "..."

  In the PR body, pre-empt the licence question: gedih3 ships under
  LicenseRef-UMD-Source-Available-NonCommercial-1.0, which is source-available
  and non-commercial rather than OSI-approved. conda-forge's stated bar is that
  the licence "allows redistribution" — LICENSE lines 57-61 permit redistribution
  of unmodified copies explicitly "through public software package repositories
  and their mirrors (and the packaging/build recipes used to do so)", and the
  recipe carries both LICENSE and NOTICE via license_file.

SUBSEQUENT RELEASES — nothing to do:

  Once gedih3-feedstock exists, conda-forge's autotick bot opens a version+hash
  PR automatically within a few hours of each PyPI release. Just review and
  merge it. The in-repo recipe/meta.yaml then becomes a staging copy only — the
  feedstock is the source of truth.
```
