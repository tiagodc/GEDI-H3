"""
Guards that pyproject.toml declares every third-party module gedih3 imports.

A module that is merely *transitively* available (rasterio via rioxarray, tqdm
via pqdm, psutil via distributed, ...) imports fine in any working environment
and so passes every other test — right up until an upstream drops the edge and
a pip-only install fails at runtime. These tests compare the imports found in
the source tree against the declared dependency list, not against whatever the
current interpreter happens to have installed.
"""

import ast
import functools
import os
import sys
import tomllib
from importlib.metadata import packages_distributions

import pytest

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SRC_ROOT = os.path.join(REPO_ROOT, 'src', 'gedih3')
PYPROJECT = os.path.join(REPO_ROOT, 'pyproject.toml')

# Optional at runtime — every import site must be guarded by try/except
# ImportError with a working fallback. `osgeo` (GDAL Python bindings) has no
# PyPI wheels and cannot be a hard dependency; raster.build_vrt falls back to
# the rasterio-only VRT writer without it.
OPTIONAL_MODULES = {'osgeo'}


def _normalize(name):
    """PEP 503 normalization, so 'python-dateutil' == 'Python_DateUtil'."""
    return name.lower().replace('_', '-').replace('.', '-')


@functools.lru_cache(maxsize=1)
def _source_trees():
    """Parse every module under src/gedih3 once; return (relpath, tree) pairs."""
    trees = []
    for dirpath, _, filenames in os.walk(SRC_ROOT):
        for filename in sorted(filenames):
            if not filename.endswith('.py'):
                continue
            path = os.path.join(dirpath, filename)
            with open(path) as handle:
                trees.append((os.path.relpath(path, REPO_ROOT),
                              ast.parse(handle.read(), path)))
    return tuple(trees)


def _import_nodes(tree):
    """Yield (node, top_level_names) for every absolute import in *tree*."""
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            yield node, [alias.name.split('.')[0] for alias in node.names]
        elif isinstance(node, ast.ImportFrom) and not node.level:
            yield node, [(node.module or '').split('.')[0]]


@functools.lru_cache(maxsize=1)
def _imported_modules():
    """Top-level third-party modules imported anywhere under src/gedih3."""
    stdlib = set(sys.stdlib_module_names)
    modules = {}
    for relpath, tree in _source_trees():
        for _, names in _import_nodes(tree):
            for name in names:
                if name and name not in stdlib and name != 'gedih3':
                    modules.setdefault(name, relpath)
    return modules


@functools.lru_cache(maxsize=1)
def _declared_distributions():
    """Distribution names in pyproject's [project] dependencies, normalized."""
    with open(PYPROJECT, 'rb') as handle:
        pyproject = tomllib.load(handle)
    declared = set()
    for spec in pyproject['project']['dependencies']:
        # 'duckdb >=1.4.4,<1.5  # comment' -> 'duckdb'
        name = spec.split('#')[0].strip()
        for delimiter in ('>', '<', '=', '!', '~', '[', ';', ' '):
            name = name.split(delimiter)[0]
        declared.add(_normalize(name))
    return frozenset(declared)


class TestDeclaredDependencies:

    def test_every_import_is_declared(self):
        """No module may reach gedih3 only through a transitive edge."""
        declared = _declared_distributions()
        module_to_dists = packages_distributions()

        undeclared = []
        for module, source in sorted(_imported_modules().items()):
            if module in OPTIONAL_MODULES:
                continue
            dists = module_to_dists.get(module)
            if dists is None:
                pytest.skip(f"'{module}' not installed; cannot map it to a distribution")
            if not any(_normalize(d) in declared for d in dists):
                undeclared.append(f"{module} (from {'/'.join(dists)}) imported in {source}")

        assert not undeclared, (
            "Modules imported by gedih3 but not declared in pyproject.toml "
            "dependencies:\n  " + "\n  ".join(undeclared)
        )

    def test_optional_imports_are_guarded(self):
        """Every OPTIONAL_MODULES import must sit under a try/except ImportError."""
        unguarded = []
        for relpath, tree in _source_trees():
            guarded = set()
            for node in ast.walk(tree):
                if not isinstance(node, ast.Try):
                    continue
                catches_import = any(
                    (isinstance(h.type, ast.Name) and h.type.id == 'ImportError')
                    or (isinstance(h.type, ast.Tuple)
                        and any(isinstance(e, ast.Name) and e.id == 'ImportError'
                                for e in h.type.elts))
                    for h in node.handlers
                )
                if catches_import:
                    guarded.update(id(child) for child in ast.walk(node))

            for node, names in _import_nodes(tree):
                if set(names) & OPTIONAL_MODULES and id(node) not in guarded:
                    unguarded.append(f"{'/'.join(names)} at {relpath}:{node.lineno}")

        assert not unguarded, (
            "Optional modules imported without an ImportError guard:\n  "
            + "\n  ".join(unguarded)
        )

    def test_recipe_matches_pyproject(self):
        """The conda recipe's run deps must not drift from pyproject's."""
        recipe_path = os.path.join(REPO_ROOT, 'recipe', 'meta.yaml')
        if not os.path.exists(recipe_path):
            pytest.skip('no conda recipe')

        with open(recipe_path) as handle:
            lines = handle.read().splitlines()

        run_deps, in_run = set(), False
        for line in lines:
            if line.strip() == 'run:':
                in_run = True
                continue
            if in_run:
                if line.strip().startswith('- '):
                    run_deps.add(_normalize(line.strip()[2:].split()[0]))
                elif line.strip() and not line.startswith((' ' * 4, '\t')):
                    break

        # Names that legitimately differ between PyPI and conda-forge, plus
        # conda-only extras (python itself, the optional GDAL bindings).
        aliases = {'h3': 'h3-py', 'duckdb': 'python-duckdb'}
        conda_only = {'python', 'gdal'}

        expected = {aliases.get(d, d) for d in _declared_distributions()}
        missing = expected - run_deps
        extra = run_deps - expected - conda_only

        assert not missing, f"In pyproject but missing from recipe/meta.yaml: {sorted(missing)}"
        assert not extra, f"In recipe/meta.yaml but not in pyproject: {sorted(extra)}"
