# Contributing to gedih3

Thank you for your interest in contributing to gedih3! This document provides guidelines for contributing to the project.

## Licensing of contributions — please read first

`gedih3` is **source available, not open source**. It is distributed under the UMD Source Available
Non-Commercial End User License Agreement ([LICENSE](LICENSE), [NOTICE](NOTICE)). Two terms directly
affect contributors, and we would rather you know them up front than discover them later:

- **Contributions become the University of Maryland's property.** Under Section 6(b), contributions,
  commits, deposits, modifications, and forks that you provide to the repository become part of the
  Materials and are owned by the University as derivative work. You retain no ownership or
  intellectual property rights in them and receive no compensation. Section 6(a) vests all right,
  title, and interest in the Materials in the University.
- **Modifications must be contributed back, not released separately.** Section 2(a) permits you to
  modify the software for non-commercial purposes and to share those modifications *only* by
  contributing them back — fork the repository and open a pull request. Distributing modified copies
  as a separate release is not permitted.

By opening a pull request you confirm that you have read these terms and that you have the right to
contribute the work you are submitting.

Redistribution of **unmodified** copies is permitted, including through public package repositories
and their mirrors, provided the LICENSE and all required notices travel with every copy.

Commercial use requires a separate license from the University of Maryland. Contact UM Ventures at
[umdtechtransfer@umd.edu](mailto:umdtechtransfer@umd.edu).

## Getting Started

1. Fork the repository and clone your fork
2. Create a conda environment:
   ```bash
   conda env create -f environment.yml
   conda activate gedih3
   pip install -e ".[test]"
   ```
3. Create a feature branch from `main`

## Development Workflow

1. Make your changes in a feature branch
2. Add or update tests as needed
3. Run the test suite:
   ```bash
   pytest tests/ -m "not integration and not slow"
   ```
4. Ensure code passes linting:
   ```bash
   ruff check src/
   ```
5. Submit a pull request against `main`

## Code Style

- Follow existing project conventions
- Use [ruff](https://docs.astral.sh/ruff/) for linting (configured in `pyproject.toml`)
- Line length limit: 120 characters
- Add docstrings in [numpydoc](https://numpydoc.readthedocs.io/) format for public functions

## Reporting Issues

- Use [GitHub Issues](https://github.com/tiagodc/GEDI-H3/issues) to report bugs or request features
- Include a minimal reproducible example when reporting bugs
- Specify your Python version, OS, and gedih3 version

## Pull Requests

- Keep PRs focused on a single change
- Reference any related issues
- Include a brief description of what changed and why
- Ensure all tests pass before requesting review

## Testing

- Unit tests go in `tests/`
- Mark tests that require network access with `@pytest.mark.integration`
- Mark slow tests with `@pytest.mark.slow`

## Questions?

Open a [discussion](https://github.com/tiagodc/GEDI-H3/discussions) or issue if you have questions about contributing.
