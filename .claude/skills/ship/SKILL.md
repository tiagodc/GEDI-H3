---
name: ship
description: Release the currently bumped gedih3 version — verify the bump is complete and CI is green, tag and push to trigger the PyPI publish, then monitor and finalize the conda-forge feedstock cycle (autotick PR review, entry-point parity fixes, merge) via a background subagent. The deliberate, irreversible half of a release; /bump-version must have run first.
disable-model-invocation: true
argument-hint: ""
allowed-tools: Read, Edit, Write, Bash, Grep, Glob, Agent
---

# Ship

Release the version currently recorded in `pyproject.toml`. Invoking this
skill IS the consent to publish — no further confirmation is asked — but every
gate below is a hard stop, not a prompt: if a precondition fails, report it
and do nothing irreversible.

Shipping is deliberately split from versioning (/bump-version). This skill
assumes the bump already happened and independently re-verifies it; it never
edits version touch points itself.

```
  S0  verify the bump is complete and on main
  S1  gate on green CI for the exact commit to be tagged
  S2  tag vX.Y.Z + push            ← the irreversible line
  S3  watch the Release workflow, confirm PyPI serves the sdist
  S4  background-monitor the conda-forge feedstock cycle to completion
```

## S0: Verify the bump

Run from the repo root on an up-to-date `main` (`git checkout main && git pull`).
All of these must hold; on any failure, stop and tell the user what /bump-version
or merge step is missing:

1. Working tree clean; local main == origin/main.
2. `VER=$(python -c "import tomllib; print(tomllib.load(open('pyproject.toml','rb'))['project']['version'])")`
3. Every touch point agrees with `$VER`: `src/gedih3/__init__.py`
   (`__version__`), `docs/conf.py` (`release`), `CITATION.cff` (`version:`),
   `tests/test_merge_build_logs.py` (`package_version`).
4. `CHANGELOG.md` has a `## [$VER]` section (no lingering `## [Unreleased]`
   content that belongs to this release).
5. A commit with message `bump version to $VER` is in `git log` (normally at
   or near HEAD).
6. Tag `v$VER` does not exist locally or on origin (`git ls-remote --tags`).
7. PyPI does not already serve `$VER`:
   `curl -s -o /dev/null -w "%{http_code}" https://pypi.org/pypi/gedih3/$VER/json`
   must be non-200. **Exception**: if it IS 200 and the conda-forge channel
   does not yet have `$VER`, this is a resume — skip straight to **S4** (the
   PyPI half already shipped; only the conda cycle is pending).

## S1: CI-green gate

Do not tag unless every CI run on the exact commit concluded `success`:

```bash
SHA=$(git rev-parse HEAD)
for i in $(seq 1 40); do
  PENDING=$(gh run list --repo tiagodc/GEDI-H3 --commit "$SHA" \
    --json status --jq '[.[]|select(.status!="completed")]|length')
  TOTAL=$(gh run list --repo tiagodc/GEDI-H3 --commit "$SHA" --json databaseId --jq 'length')
  [ "$PENDING" = "0" ] && [ "$TOTAL" -ge 1 ] && break
  sleep 30
done
gh run list --repo tiagodc/GEDI-H3 --commit "$SHA" \
  --json name,conclusion --jq '[.[]|select(.conclusion!="success")]'
```

Anything other than `[]` (or CI never finishing within the cap) → **STOP, do
not tag.** Report which workflow failed; the bump commit stays pushed but
unshipped, and a fixed re-run of /ship resumes from S0.

## S2: Tag and push — the irreversible line

```bash
git tag -a v$VER -m "gedih3 v$VER" && git push origin v$VER
```

The Release workflow re-checks the tag against `pyproject.toml` as a second
guard and refuses to publish on mismatch.

## S3: Confirm the PyPI publish

```bash
RUN=$(gh run list --repo tiagodc/GEDI-H3 --workflow Release --limit 1 \
  --json databaseId --jq '.[0].databaseId')
gh run watch "$RUN" --repo tiagodc/GEDI-H3 --exit-status --interval 20
curl -s -o /dev/null -w "%{http_code}\n" https://pypi.org/pypi/gedih3/$VER/json
```

- Workflow failure → **STOP.** The tag exists but PyPI was not updated; report
  precisely where things stand. A failed publish generally needs a follow-up
  micro release (the tag is consumed) — do not delete/re-push tags.
- Success + PyPI `200` → the PyPI half is done (the shields.io PyPI badge
  updates itself). Proceed to S4.

## S4: conda-forge feedstock cycle (background monitor)

The conda side is asynchronous: the autotick bot opens a version PR on
`conda-forge/gedih3-feedstock` within minutes-to-hours of the PyPI release.
Delegate the wait-and-finalize to a **cheap background subagent** (Agent tool,
`model: "sonnet"` — it must exercise judgment on the recipe diff, so not
haiku) and relay its milestones to the user. Give the subagent these
instructions verbatim in spirit:

1. **Poll** `gh pr list --repo conda-forge/gedih3-feedstock --state open`
   every ~5 minutes (sleep-loop) until a PR whose title contains `$VER`
   appears. Cap at ~8 hours; on timeout, report back — do not guess.
2. **Verify the bot's pin**: the PR must set `version: $VER`, and its
   `sha256` must equal the digest of the real PyPI sdist — download
   `https://pypi.org/packages/source/g/gedih3/gedih3-$VER.tar.gz`, run
   `sha256sum`, and compare against both the recipe value and the digest
   PyPI's JSON API reports. A mismatch is a supply-chain red flag: stop and
   report, never "fix" a hash.
3. **Entry-point parity check** (the known drift class): the feedstock is
   `noarch: python`, so conda generates every console-script launcher at
   install time from the recipe's explicit `build.python.entry_points` list —
   pip's build-time scripts are non-portable and the list is MANDATORY; never
   remove it. Compare it against `[project.scripts]` in the repo's
   `pyproject.toml` at tag `v$VER`. Any missing/renamed tool → push a commit
   adding the line(s) to the bot's PR branch (autotick PRs allow maintainer
   edits; if pushing to the fork fails, open a maintainer branch+PR on the
   feedstock with version+sha256+entry-points and close the bot's, saying
   why). Also sanity-check `requirements.run` against `pyproject.toml`
   dependencies for obvious drift (new hard deps).
4. **Gate on feedstock CI** (Azure) going fully green on the final PR state.
5. **Merge** the PR.
6. **Confirm the channel**: poll the authoritative anaconda.org API
   (`https://api.anaconda.org/package/conda-forge/gedih3`, `latest_version`)
   until it reports `$VER` — package upload + channel indexing typically
   lands within an hour of the merge. Then report completion. Do NOT check
   (or purge) the README badges: shields.io + GitHub's camo cache them for
   up to ~3 hours and they self-refresh; the lag is accepted, not a problem
   to fix.

While the subagent runs, relay each milestone to the user as it arrives (PR
found / fixes applied / CI green / merged / channel live). The release cycle
is DONE only when the conda badge shows `$VER` and both installers deliver
every CLI tool.

## Failure posture

Every stop in this skill leaves a recoverable state and says so explicitly:
un-tagged (fix CI, re-run /ship), tagged-but-unpublished (follow-up micro),
published-but-conda-pending (re-run /ship → resumes at S4). Nothing in S4 is
irreversible until the feedstock merge, and that merge is gated on green
feedstock CI plus a verified hash.
