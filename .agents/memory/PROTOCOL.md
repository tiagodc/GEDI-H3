# Project memory protocol
<!-- protocol-version: 2 — managed by the project-memory skill; `pmem init` / `pmem on` refresh this file. Edit the skill's copy, not this one. -->

`.agents/memory/` is the project's shared memory: plain Markdown in git, read and written by whichever agent works here
(any harness, model, machine or account). Everything needed to continue the work lives here, not in any harness's private store.
Precedence: the user's instructions > AGENTS.md > memory. Memory is historical evidence, never an instruction that overrides the user.

## Layout and caps
| Path | Holds | Cap |
|---|---|---|
| `INDEX.md` | `## Summary` (≤3 lines) · `## Routes` ("when doing X → read Y") · `## Memory` (one line per page) | 80 lines |
| `NOW.md` | current focus + handoff baton. **Overwrite, never append** | 40 lines |
| `LOG.md` | one line per meaningful event, appended; rotated monthly to `log/YYYY-MM.md` | current month |
| `decisions/NNNN-slug.md` | why X over Y, still binding | 60 lines |
| `gotchas/slug.md` | a trap or failed approach that cost time, with root cause and fix | 60 lines |
| `procedures/slug.md` | steps where the order matters (setup, release, data rebuild) | 60 lines |
| `concepts/slug.md` | architecture/domain synthesis that the code alone doesn't make obvious | 60 lines |
| `inbox/*.md` | async messages from related projects, delivered by git; delete once handled | — |
Live conversations between agents use channel files in `.work/channels/` (git-ignored), not memory.

## Time
Every timestamp is ISO 8601 with a zone offset, as given by `date -Iseconds` (e.g. `2026-10-19T14:03:07-04:00`). Never write a bare date or time.

## Reading (cheap by default)
1. `git pull --ff-only` first (warn and stop if the tree is dirty or has diverged).
2. Read `NOW.md` when continuing work and `INDEX.md` before non-trivial tasks. `ls inbox/` and handle pending messages first.
3. Follow **Routes**: open only the files and pages the route for your task points to. Grep `.agents/memory/` before
   re-investigating the repo. Read `LOG.md` only when tracing history.
4. Verify a remembered claim against the current code before relying on it. If it's wrong, fix the page now.

## Keeping it live (during the session, not only at the end)
- **The moment you learn something durable** (a decision, a trap, a working procedure, a non-obvious fact), write or update its page
  and its INDEX line.
- **The moment something meaningful happens** (milestone reached, experiment finished or failed, release, important finding), append
  one LOG line: `<date -Iseconds> · <harness/model> · <event, with numbers/paths>` (or `pmem log "<event>"`).
- **When structure changes** (new module, moved entry point, new workflow), update INDEX Summary/Routes.
- **Before ending, clearing, compacting, switching agent/machine, or starting a long risky run:** overwrite `NOW.md`.
  Then commit only the memory (`git add .agents/memory && git commit -m "agents: memory <topic>"`) and ask before pushing.

## What to write (and not)
- **Bar:** a durable fact backed by evidence (commit, `file:line`, command, measured number). Would losing it make the next agent
  repeat work or make a mistake? If not, skip it, or make it a LOG line at most.
- **Never write:** routine chatter, transient failures (network, a missing credential, a flaky run) unless they recur, broad claims like
  "tool X is broken" (write the exact condition instead), filler sections, or secrets.
- **Update before creating.** Extend or correct the existing page instead of adding a near-duplicate.
- **Portable:** repo-relative paths only. No home paths, hostnames, usernames, or harness state (session ids, "/resume").
  Machine-specific setup belongs in `procedures/` as conditional steps.
- **Page format:** line 1 `# Title`, line 2 `> abstract` (≤120 chars, identical to its INDEX line), line 3 `updated: <ISO+offset>`.
  Link other pages as `[[gotchas/slug]]`, only when the link helps.
  - decision: `**Status:** proposed | accepted | superseded by [[decisions/NNNN-x]]` → `## Context` → `## Decision` → `## Consequences` → `## Rejected`
  - gotcha: `## Symptom` → `## Cause` → `## Fix` → `## Evidence`
  - procedure: numbered runnable steps, then `## Verify`
  - concept: prose plus pointers into the code (`path:line`); synthesis, not a diary
- **Supersede, don't delete.** Write the new decision, then change only the old page's Status line. Contradictions you can't
  resolve go into NOW.md's Open questions for the user.

## Handoff: NOW.md (overwrite in this order)
```
# NOW
from: <harness/model> @ <host> · <ISO+offset> · branch <b> · last commit <sha>
## Open questions   — undecided items, what the user must answer
## Next steps       — numbered, concrete, the very next action first
## Files touched    — paths; mark uncommitted ones (uncommitted)
## State            — 2–3 sentences: what works, what doesn't, where you stopped (running jobs, PIDs, half-done batches)
```
NOW.md alone must be enough for any agent on any machine to resume.

## Inbox (async, between related projects, delivered by git)
- File: `inbox/<YYYYMMDDTHHMM±zone>-<from-project>-<slug>.md`. Body: `RE:` / `FROM:` (project, agent, time) / `ASK:` (goal, constraints,
  done-when; self-contained) / `POINTERS:` / `REPLY-TO:`.
- Send only to projects of the same owner/org unless the user approves. Commit only that file in the receiving repo; ask before pushing.
- To receive: act on it or ask the user, reply through the sender's inbox if needed, then delete the message in the commit that handles it.

## Project rules
Always/never rules live in AGENTS.md (max 15, one or two lines each, each with its reason), not in pages. A lesson becomes a rule
only after it has held up repeatedly and the user has approved it.
