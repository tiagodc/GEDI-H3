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
import re
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
def _declared_floors():
    """Map normalized distribution name -> declared lower bound (None if unpinned).

    Single parse of pyproject's [project] dependencies, shared by every check
    below so the three files can only be compared against one reading of the
    source of truth.
    """
    with open(PYPROJECT, 'rb') as handle:
        pyproject = tomllib.load(handle)
    floors = {}
    for spec in pyproject['project']['dependencies']:
        # 'duckdb >=1.4.4,<1.5  # comment' -> name 'duckdb', floor '1.4.4'
        spec = spec.split('#')[0].strip()
        name = spec
        for delimiter in ('>', '<', '=', '!', '~', '[', ';', ' '):
            name = name.split(delimiter)[0]
        bound = re.search(r'>=\s*([0-9][^,\s]*)', spec)
        floors[_normalize(name)] = bound.group(1) if bound else None
    return floors


def _declared_distributions():
    """Distribution names in pyproject's [project] dependencies, normalized."""
    return frozenset(_declared_floors())


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

    def test_constraints_min_matches_declared_floors(self):
        """constraints-min.txt must pin exactly the declared lower bounds.

        The CI minimum-versions job installs from that file, so any drift means
        it silently stops testing what pyproject actually claims to support.
        """
        constraints_path = os.path.join(REPO_ROOT, 'constraints-min.txt')
        if not os.path.exists(constraints_path):
            pytest.skip('no constraints-min.txt')

        with open(constraints_path) as handle:
            pinned = {}
            for line in handle:
                line = line.split('#')[0].strip()
                if not line:
                    continue
                name, _, version = line.partition('==')
                pinned[_normalize(name)] = version

        floors = _declared_floors()

        missing = sorted(set(floors) - set(pinned))
        extra = sorted(set(pinned) - set(floors))
        assert not missing, f"declared in pyproject but not pinned in constraints-min.txt: {missing}"
        assert not extra, f"pinned in constraints-min.txt but not a dependency: {extra}"

        mismatched = [
            f'{name}: pyproject floor {floors[name]} != pinned {pinned[name]}'
            for name in sorted(floors)
            if floors[name] is not None and floors[name] != pinned[name]
        ]
        assert not mismatched, 'constraints-min.txt has drifted:\n  ' + '\n  '.join(mismatched)

    def test_recipe_matches_pyproject(self):
        """The conda recipe's run deps must not drift from pyproject's."""
        recipe_path = os.path.join(REPO_ROOT, 'recipe', 'meta.yaml')
        if not os.path.exists(recipe_path):
            pytest.skip('no conda recipe')

        with open(recipe_path) as handle:
            lines = handle.read().splitlines()

        # name -> declared lower bound (None when unpinned)
        run_deps, in_run = {}, False
        for line in lines:
            stripped = line.strip()
            if stripped == 'run:':
                in_run = True
                continue
            if in_run:
                if stripped.startswith('- '):
                    body = stripped[2:]
                    bound = re.search(r'>=\s*([0-9][^,\s]*)', body)
                    run_deps[_normalize(body.split()[0])] = bound.group(1) if bound else None
                elif stripped and not stripped.startswith('#') \
                        and not line.startswith((' ' * 4, '\t')):
                    break

        # Names that legitimately differ between PyPI and conda-forge, plus
        # conda-only extras (python itself, and the GDAL bindings — optional on
        # pip because build_vrt falls back, but cheap on conda where rasterio
        # already pulls libgdal-core).
        aliases = {'h3': 'h3-py', 'duckdb': 'python-duckdb'}
        conda_only = {'python', 'gdal'}

        floors = {aliases.get(name, name): floor
                  for name, floor in _declared_floors().items()}

        missing = set(floors) - set(run_deps)
        extra = set(run_deps) - set(floors) - conda_only
        assert not missing, f"In pyproject but missing from recipe/meta.yaml: {sorted(missing)}"
        assert not extra, f"In recipe/meta.yaml but not in pyproject: {sorted(extra)}"

        # Names alone are not enough: a floor corrected in pyproject and not
        # mirrored here would let conda resolve a version the project has
        # declared unsupported, and nothing else would notice.
        drifted = [
            f'{name}: pyproject >={floors[name]}  recipe >={run_deps[name]}'
            for name in sorted(floors)
            if floors[name] is not None and floors[name] != run_deps[name]
        ]
        assert not drifted, 'recipe/meta.yaml floors have drifted:\n  ' + '\n  '.join(drifted)
