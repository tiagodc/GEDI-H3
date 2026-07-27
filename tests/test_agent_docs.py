"""
Guards that CLAUDE.md and .claude/** stay true to the source tree.

These files are loaded into an AI coding agent's context at the start of every
session, so a stale claim in them is worse than no claim at all — the agent has
no way to know it is wrong and acts on it. The historical failure mode in this
repository was uniform: hand-maintained inventories. Line-count annotations that
drifted 2-3x, a CLI list that named ten of thirteen entry points, an exception
count off by one, a pointer to a test runner that had been deleted. None of the
*prose about design intent* ever went stale; every stale claim was a count, a
list, or a path.

So these tests police exactly that. They deliberately do not try to verify
narrative accuracy, which is unfalsifiable and would produce a brittle test.
"""

import ast
import functools
import glob
import os
import re
import tomllib

import pytest

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SRC_ROOT = os.path.join(REPO_ROOT, 'src', 'gedih3')
PYPROJECT = os.path.join(REPO_ROOT, 'pyproject.toml')
CLAUDE_MD = os.path.join(REPO_ROOT, 'CLAUDE.md')
CLAUDE_DIR = os.path.join(REPO_ROOT, '.claude')
RULES_DIR = os.path.join(CLAUDE_DIR, 'rules')
HELPER_SKILL = os.path.join(CLAUDE_DIR, 'skills', 'find-existing-helper', 'SKILL.md')
CLI_REFERENCE = os.path.join(REPO_ROOT, 'docs', 'user-guide', 'cli-reference.md')

# Anthropic's guidance for the Claude 5 generation is to keep a CLAUDE.md under
# 200 lines: it loads in full on every request and is injected into every custom
# subagent, and longer files measurably reduce adherence. Raising this number is
# almost never the right fix — move the content to a path-scoped rule in
# .claude/rules/ (loads only when a matching file is opened) or to a skill
# (loads only on demand).
CLAUDE_MD_MAX_LINES = 200
RULE_MAX_LINES = 150

# Paths referenced deliberately even though they do not currently exist.
PATH_ALLOWLIST = {
    # The in-repo conda recipe was retired when the conda-forge feedstock became
    # the source of truth. /bump-version still describes what to do "only if it
    # exists", so that a recipe returning to the repo is handled rather than
    # silently skipped. The reference is conditional by design, not stale.
    'recipe/meta.yaml',
}

# Machine- or site-specific strings that must never reach a tracked agent file.
# This repository is public; a hardcoded interpreter path inside one
# contributor's cluster environment is unusable to everyone else.
FORBIDDEN_PATTERNS = [
    (r'/gpfs/', 'an absolute path into a specific HPC filesystem'),
    (r'\bgh3_dev\b', "a contributor's local conda environment name (the public one is `gedih3`)"),
    (r'C:\\\\ProgramData', 'a Windows machine-local path'),
    (r'/c/ProgramData', 'a Windows machine-local path'),
    (r'/home/[a-z][a-z0-9_-]*/', "a specific user's home directory"),
]

# Hand-maintained counts. Every one of these that has ever appeared in this
# repository's agent files was wrong within two releases.
INVENTORY_PATTERNS = [
    (r'~?\d+\s*LOC\b', 'a line-count annotation'),
    (r'\b\d+\s+(?:CLI\s+)?entry[- ]points?\b', 'a count of entry points'),
    (r'\b\d+\s+CLI\s+tools?\b', 'a count of CLI tools'),
    (r'\b\d+\s+exception\s+types?\b', 'a count of exception types'),
    (r'\b\d+\s+(?:sub-?agents?|skills|diagnoses)\b', 'a count of agent-config files'),
    (r'\(~\d{3,}\s+lines?\)', 'a line-count annotation'),
]


def _read(path):
    with open(path, encoding='utf-8') as handle:
        return handle.read()


def _tracked_agent_files():
    """CLAUDE.md plus every markdown/json file under .claude/ that is tracked.

    settings.local.json is gitignored and machine-specific by design, so it is
    exempt from the hygiene checks below.
    """
    files = [CLAUDE_MD]
    for pattern in ('rules/*.md', 'skills/*/SKILL.md', 'agents/*.md', 'settings.json'):
        files.extend(sorted(glob.glob(os.path.join(CLAUDE_DIR, pattern))))
    return [f for f in files if os.path.exists(f)]


def _frontmatter(text):
    """Return the raw YAML frontmatter block, or None."""
    match = re.match(r'^---\n(.*?)\n---\n', text, re.S)
    return match.group(1) if match else None


@functools.lru_cache(maxsize=None)
def _module_symbols(relpath):
    """Top-level names defined or imported by one module under src/gedih3."""
    full = os.path.join(SRC_ROOT, relpath)
    if not os.path.exists(full):
        return None
    tree = ast.parse(_read(full), filename=full)
    names = set()
    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            names.add(node.name)
        elif isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name):
                    names.add(target.id)
        elif isinstance(node, (ast.Import, ast.ImportFrom)):
            for alias in node.names:
                names.add(alias.asname or alias.name.split('.')[0])
    return names


@functools.lru_cache(maxsize=1)
def _entry_points():
    with open(PYPROJECT, 'rb') as handle:
        return set(tomllib.load(handle).get('project', {}).get('scripts', {}))


@functools.lru_cache(maxsize=1)
def _public_api_names():
    """Every gh3_-prefixed symbol defined anywhere in the package.

    Parsed from source rather than imported, so this test does not depend on
    gedih3 being installed or its dependencies being present.
    """
    names = set()
    for dirpath, _, filenames in os.walk(SRC_ROOT):
        for filename in filenames:
            if not filename.endswith('.py'):
                continue
            rel = os.path.relpath(os.path.join(dirpath, filename), SRC_ROOT)
            names |= {n for n in (_module_symbols(rel) or ()) if n.startswith('gh3_')}
    return names


class TestHelperIndex:
    """The find-existing-helper skill is the anti-reinvention index.

    Its second column names the module a helper lives in. That column is the
    machine-checkable contract: if it drifts, an agent is sent to the wrong file
    and concludes the helper does not exist.
    """

    def test_skill_exists(self):
        assert os.path.exists(HELPER_SKILL), (
            'find-existing-helper/SKILL.md is missing. It holds the helper index that '
            'CLAUDE.md points at instead of carrying inline.'
        )

    def test_every_indexed_symbol_resolves(self):
        rows = re.findall(r'^\|\s*(`[^|]+`)\s*\|\s*`([^`]+)`\s*\|', _read(HELPER_SKILL), re.M)
        assert rows, 'no helper table rows parsed — has the table format changed?'

        missing = []
        for names_cell, module in rows:
            symbols = _module_symbols(module)
            if symbols is None:
                missing.append(f'module not found: src/gedih3/{module}')
                continue
            for name in re.findall(r'`([A-Za-z_][A-Za-z0-9_]*)`', names_cell):
                if name not in symbols:
                    missing.append(f'{name} is not defined in src/gedih3/{module}')

        assert not missing, (
            'find-existing-helper/SKILL.md names helpers that no longer resolve:\n  '
            + '\n  '.join(missing)
            + '\nUpdate the table, or move the row if the helper was relocated.'
        )


class TestCliParity:
    """Agent files and the public reference must agree with [project.scripts]."""

    def test_no_agent_file_names_a_nonexistent_tool(self):
        # The gh3_ prefix is shared by the CLI entry points and by public Python
        # API functions (gh3_load, gh3_read_meta, gh3_select_partitions, ...), so
        # a name is only suspect if it is neither.
        known = _entry_points() | _public_api_names()
        bad = []
        for path in _tracked_agent_files():
            for name in set(re.findall(r'\bgh3_[a-z_]+\b', _read(path))):
                if name not in known:
                    bad.append(f'{os.path.relpath(path, REPO_ROOT)} names `{name}`')
        assert not bad, (
            'agent files reference gh3_* names that are neither a declared entry point '
            'nor a symbol in the package:\n  ' + '\n  '.join(sorted(bad))
            + '\nThis is how a renamed tool goes unnoticed — gh3_list_variables outlived '
            'its rename to gh3_read_schema in a docs page for months.'
        )

    def test_every_entry_point_is_publicly_documented(self):
        reference = _read(CLI_REFERENCE)
        undocumented = sorted(name for name in _entry_points() if f'`{name}`' not in reference)
        assert not undocumented, (
            'these entry points have no section in docs/user-guide/cli-reference.md: '
            + ', '.join(undocumented)
            + '\nA tool users cannot find is a tool that does not exist. Adding one '
            'also means adding it to the conda-forge feedstock entry_points.'
        )


class TestReferencedPaths:
    """A pointer to a file that was deleted is the canonical stale claim."""

    def test_referenced_repo_paths_exist(self):
        pattern = re.compile(r'`((?:src|tests|docs|recipe|scripts)/[A-Za-z0-9_./*-]+)`')
        missing = []
        for path in _tracked_agent_files():
            for ref in sorted(set(pattern.findall(_read(path)))):
                if ref in PATH_ALLOWLIST or '*' in ref:
                    continue
                if not os.path.exists(os.path.join(REPO_ROOT, ref)):
                    missing.append(f'{os.path.relpath(path, REPO_ROOT)} -> {ref}')
        assert not missing, (
            'agent files point at paths that do not exist:\n  ' + '\n  '.join(missing)
        )


class TestRulesAreScoped:
    """An unscoped rule saves nothing; a typo'd glob makes a rule invisible.

    Rules without `paths:` frontmatter load unconditionally, at the same
    priority as CLAUDE.md — which defeats the reason for splitting them out. A
    rule whose glob matches nothing is worse: it fails silently, with no error
    and no warning, and the knowledge simply never reaches the agent.
    """

    def _rule_files(self):
        return sorted(glob.glob(os.path.join(RULES_DIR, '*.md')))

    def test_rules_directory_is_populated(self):
        assert self._rule_files(), (
            '.claude/rules/ is empty. CLAUDE.md delegates subsystem detail to it.'
        )

    @pytest.mark.parametrize('rule', sorted(glob.glob(os.path.join(RULES_DIR, '*.md'))),
                             ids=lambda p: os.path.basename(p))
    def test_rule_has_globs_that_match_real_files(self, rule):
        front = _frontmatter(_read(rule))
        assert front is not None, f'{os.path.basename(rule)} has no YAML frontmatter'

        globs = re.findall(r'^\s*-\s*["\']?([^"\'\n]+)["\']?\s*$', front, re.M)
        assert globs, (
            f'{os.path.basename(rule)} declares no `paths:` globs, so it loads on every '
            'session at CLAUDE.md priority — which is the cost the split exists to avoid.'
        )

        dead = [g for g in globs
                if not glob.glob(os.path.join(REPO_ROOT, g), recursive=True)]
        assert not dead, (
            f'{os.path.basename(rule)} has globs matching no files: {dead}\n'
            'The rule would never load, silently.'
        )


class TestNoDriftProneContent:

    @pytest.mark.parametrize('path', _tracked_agent_files(),
                             ids=lambda p: os.path.relpath(p, REPO_ROOT))
    def test_no_hand_maintained_inventories(self, path):
        text = _read(path)
        found = []
        for pattern, what in INVENTORY_PATTERNS:
            for match in re.finditer(pattern, text, re.I):
                line = text[:match.start()].count('\n') + 1
                found.append(f'line {line}: {match.group(0)!r} is {what}')
        assert not found, (
            f'{os.path.relpath(path, REPO_ROOT)} contains hand-maintained counts:\n  '
            + '\n  '.join(found)
            + '\nName the symbol or the source of truth and let the reader grep. '
            'Every count this repository has written into an agent file went stale.'
        )

    @pytest.mark.parametrize('path', _tracked_agent_files(),
                             ids=lambda p: os.path.relpath(p, REPO_ROOT))
    def test_no_machine_local_paths(self, path):
        text = _read(path)
        found = []
        for pattern, what in FORBIDDEN_PATTERNS:
            for match in re.finditer(pattern, text):
                line = text[:match.start()].count('\n') + 1
                found.append(f'line {line}: {match.group(0)!r} is {what}')
        assert not found, (
            f'{os.path.relpath(path, REPO_ROOT)} is tracked and public but contains '
            f'machine-local references:\n  ' + '\n  '.join(found)
            + '\nPut these in .claude/settings.local.json or CLAUDE.local.md, '
            'both gitignored.'
        )


class TestSizeBudgets:

    def test_claude_md_stays_small(self):
        lines = len(_read(CLAUDE_MD).splitlines())
        assert lines <= CLAUDE_MD_MAX_LINES, (
            f'CLAUDE.md is {lines} lines, over the {CLAUDE_MD_MAX_LINES}-line budget.\n'
            'It loads in full on every request and into every custom subagent, and '
            'adherence drops as it grows. Raising this limit is almost never the fix — '
            'move the content to a path-scoped rule in .claude/rules/ (loads only when a '
            'matching file is opened) or to a skill (loads only on demand).'
        )

    @pytest.mark.parametrize('rule', sorted(glob.glob(os.path.join(RULES_DIR, '*.md'))),
                             ids=lambda p: os.path.basename(p))
    def test_rules_stay_focused(self, rule):
        lines = len(_read(rule).splitlines())
        assert lines <= RULE_MAX_LINES, (
            f'{os.path.basename(rule)} is {lines} lines, over the {RULE_MAX_LINES}-line '
            'budget. Split it, or move reference material into a skill.'
        )


class TestSkillFrontmatter:

    @pytest.mark.parametrize(
        'skill', sorted(glob.glob(os.path.join(CLAUDE_DIR, 'skills', '*', 'SKILL.md'))),
        ids=lambda p: os.path.basename(os.path.dirname(p)))
    def test_skill_declares_name_and_description(self, skill):
        front = _frontmatter(_read(skill))
        assert front is not None, f'{skill} has no YAML frontmatter'

        name = re.search(r'^name:\s*(\S+)', front, re.M)
        assert name, 'skill frontmatter must declare `name`'
        expected = os.path.basename(os.path.dirname(skill))
        assert name.group(1) == expected, (
            f'skill `name:` is {name.group(1)!r} but its directory is {expected!r}; '
            'they must match for /<name> invocation to work'
        )

        assert re.search(r'^description:\s*\S', front, re.M), (
            'skill frontmatter must declare `description` — it is the only text Claude '
            'sees when deciding whether the skill is relevant'
        )
