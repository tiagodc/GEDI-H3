<!-- Thanks for contributing to gedih3. Please read CONTRIBUTING.md first — the
     licensing section in particular, since it affects ownership of your work. -->

## What changed and why

<!-- One or two sentences. Link any related issue. -->

## How it was verified

<!-- Which tests you ran, or the manual reproduction. "Seems right" is not
     verification — if it touches build, update, or export, say which invariant
     from tests/TESTING.md you checked. -->

- [ ] `pytest tests/ -m "not integration and not slow"` passes
- [ ] `ruff check src/` passes
- [ ] Tests added or updated for the change

## Checklist

- [ ] If a dependency was added: declared in `pyproject.toml`, given a lower
      bound pinned in `constraints-min.txt`, and mirrored in the
      [conda-forge feedstock](https://github.com/conda-forge/gedih3-feedstock)
- [ ] If a CLI entry point was added: declared in `[project.scripts]`,
      documented in `docs/user-guide/cli-reference.md`, and added to the
      feedstock's `entry_points`
- [ ] I have read the licensing section of [CONTRIBUTING.md](../CONTRIBUTING.md)
      and understand that contributions become the property of the University of
      Maryland under Section 6(b) of the LICENSE
