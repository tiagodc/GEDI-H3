# conda-forge recipe

The conda-forge recipe for gedih3 now lives in its own feedstock:

**https://github.com/conda-forge/gedih3-feedstock**

That feedstock is the source of truth for the conda package. On each new
release to PyPI, conda-forge's autotick bot opens a pull request there that
updates the version and sdist `sha256` automatically — review and merge it.
Only edit the `run:` dependency list by hand if a release added or changed a
dependency (`pip check` in the recipe's test phase fails the build if the list
drifts from the wheel's metadata).

Install with:

```bash
conda install -c conda-forge gedih3
```

The `meta.yaml` that previously lived here was the one-time staged-recipes
submission; it was merged on 2026-07-23 and is now maintained in the feedstock,
so it has been removed to avoid two diverging sources of truth.
