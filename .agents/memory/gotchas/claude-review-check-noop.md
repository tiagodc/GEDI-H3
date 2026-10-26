# claude-review CI check was a no-op
> claude-review CI check passed without reviewing until #43; how to test workflow changes
updated: 2026-10-26T11:12:31-04:00

## Symptom
Until PR #43 (4841d790) every `Claude Code Review` run ended after ~3–6 s, posted nothing, and went green.
The #37–#40 stack merged green; an independent review later found a silent data-loss bug in #38.

## Cause
The code-review plugin's first subagent started in the background; the headless session ended at turn end and the run exited.

## Fix
In `.github/workflows/claude-code-review.yml`: `CLAUDE_CODE_DISABLE_BACKGROUND_TASKS: "1"` in the step env, plus `--comment`,
`pull-requests: write`, and a minimal `--allowedTools`. `CLAUDE_CODE_SUBPROCESS_ENV_SCRUB: "1"` makes Claude Code refuse to
start without bubblewrap, so keep the bubblewrap install step while the scrub stays.

Still true after the fix: the review skips a PR Claude already commented on, so later pushes need their own independent look.

## Evidence
- Testing a workflow change: a PR that edits the workflow file can't test itself (the action skips runs whose workflow file
  differs from main's — its app-token check). Use a throwaway probe PR that adds `github_token: ${{ secrets.GITHUB_TOKEN }}`.
- Transcript: upload `$RUNNER_TEMP/claude-execution-output.json` as a 1-day artifact (`show_full_output` logs are
  public on this public repo), then delete the artifact.
- `CHANGELOG.md` 0.20.1 entry.
