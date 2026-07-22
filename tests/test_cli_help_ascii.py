"""
Guards that every CLI's --help output is pure ASCII.

conda-forge builds and tests on Windows, where the console encoding is a
code page (cp1252), not UTF-8. A non-ASCII character in --help breaks that
build two different ways:

  * cp1252 *cannot* encode it (e.g. the arrows → ↔) → Python raises
    UnicodeEncodeError and the command exits non-zero;
  * cp1252 *can* encode it (e.g. the em-dash — → byte 0x97) → no Python
    error, but the emitted byte is invalid UTF-8, so rattler-build's
    output reader fails with "stream did not contain valid UTF-8".

Either way the feedstock test phase fails. This test renders each entry
point's --help exactly as a terminal would and asserts the result is ASCII,
so the problem is caught here rather than in a conda-forge CI log.
"""

import subprocess
import sys

import pytest

# The entry points declared in pyproject.toml [project.scripts].
CLI_MODULES = [
    'gedih3.cli.gh3_build',
    'gedih3.cli.gh3_download',
    'gedih3.cli.gh3_extract',
    'gedih3.cli.gh3_aggregate',
    'gedih3.cli.gh3_list_resolutions',
    'gedih3.cli.gh3_read_schema',
    'gedih3.cli.gh3_rasterize',
    'gedih3.cli.gh3_update',
    'gedih3.cli.gh3_from_img',
    'gedih3.cli.gh3_from_polygon',
    'gedih3.cli.gh3_build_ducklake',
    'gedih3.cli.gh3_doctor',
]


@pytest.mark.parametrize('module', CLI_MODULES)
def test_help_output_is_ascii(module):
    """`<cli> --help` must emit only ASCII, so it survives a cp1252 console."""
    result = subprocess.run(
        [sys.executable, '-m', module, '--help'],
        capture_output=True, text=True, timeout=120,
    )
    assert result.returncode == 0, (
        f'{module} --help exited {result.returncode}:\n{result.stderr[-2000:]}'
    )

    offenders = sorted({c for c in result.stdout if ord(c) > 127})
    assert not offenders, (
        f'{module} --help contains non-ASCII characters that break the Windows '
        f'conda-forge build: '
        + ', '.join(f'{c!r} (U+{ord(c):04X})' for c in offenders)
        + '. Use ASCII equivalents in argparse help/description/epilog and in any '
        'registry description that feeds them (e.g. "->" for an arrow, " - " for '
        'an em-dash).'
    )
