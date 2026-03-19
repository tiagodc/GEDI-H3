# Contributing to gedih3

Thank you for your interest in contributing to gedih3! This document provides guidelines for contributing to the project.

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
