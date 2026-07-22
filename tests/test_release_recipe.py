"""
Guards that the conda recipe describes a real, published PyPI artifact.

The conda recipe does not build from git — it downloads the sdist that the
release workflow published to PyPI and verifies it against a pinned sha256. So
three things have to agree: the version in pyproject.toml, the version in the
recipe, and the bytes on PyPI. Nothing else in the suite checks that, and the
failure mode is quiet: a recipe carrying the previous release's hash still
parses, still lints, and only breaks at conda build time.

The hash cannot be computed ahead of the upload. sdists are not byte-identical
across machines — building the same commit locally and in CI yields different
tarballs — so the digest is only knowable once PyPI is serving the real file.
That is why `/bump-version` invalidates the hash rather than guessing it, and
why PENDING_SHA256 below is a legitimate intermediate state rather than a bug.
"""

import hashlib
import os
import re
import urllib.request

import pytest
import tomllib

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RECIPE = os.path.join(REPO_ROOT, 'recipe', 'meta.yaml')
PYPROJECT = os.path.join(REPO_ROOT, 'pyproject.toml')

# Written by /bump-version between the version bump and the PyPI upload. Must
# stay a single canonical string so it is greppable and can never be mistaken
# for a real digest.
PENDING_SHA256 = 'PENDING_PYPI_UPLOAD'


def _recipe_fields():
    """Return (version, sha256) as declared in the recipe."""
    if not os.path.exists(RECIPE):
        pytest.skip('no conda recipe')
    raw = open(RECIPE).read()
    version = re.search(r'{%\s*set\s+version\s*=\s*"([^"]+)"\s*%}', raw)
    sha = re.search(r'^\s*sha256:\s*(\S+)\s*$', raw, re.M)
    return (version.group(1) if version else None,
            sha.group(1) if sha else None)


def _pyproject_version():
    with open(PYPROJECT, 'rb') as handle:
        return tomllib.load(handle)['project']['version']


class TestRecipeSelfConsistency:
    """Offline checks — these must hold at every commit."""

    def test_recipe_version_matches_pyproject(self):
        version, _ = _recipe_fields()
        assert version == _pyproject_version(), (
            f'recipe/meta.yaml declares {version} but pyproject.toml says '
            f'{_pyproject_version()}. /bump-version updates both; a mismatch '
            f'means one was edited by hand.'
        )

    def test_sha256_is_a_real_digest_or_explicitly_pending(self):
        """A malformed or leftover hash must never look like a valid one."""
        _, sha = _recipe_fields()
        assert sha is not None, 'recipe/meta.yaml has no sha256 field'
        assert sha == PENDING_SHA256 or re.fullmatch(r'[0-9a-f]{64}', sha), (
            f'sha256 is neither a 64-char hex digest nor the canonical '
            f'{PENDING_SHA256} marker: {sha!r}'
        )

    def test_source_url_tracks_the_recipe_version(self):
        """The URL must interpolate the version, not hardcode a stale one."""
        raw = open(RECIPE).read()
        # Capture to end of line: the URL embeds `{{ name[0] }}` style
        # expressions, which contain spaces.
        url = re.search(r'^\s*url:\s*(.+?)\s*$', raw, re.M)
        assert url, 'recipe/meta.yaml has no source url'
        assert '{{ version }}' in url.group(1), (
            'source url does not interpolate {{ version }}, so bumping the '
            f'version would leave it pointing at the old sdist: {url.group(1)}'
        )


@pytest.mark.integration
class TestRecipeMatchesPublishedSdist:
    """Network checks — the recipe must describe bytes that actually exist."""

    def test_pinned_sha256_matches_the_published_sdist(self):
        version, sha = _recipe_fields()
        if sha == PENDING_SHA256:
            pytest.skip(f'{version} not yet published; hash pending by design')

        url = (f'https://pypi.org/packages/source/g/gedih3/'
               f'gedih3-{version}.tar.gz')
        with urllib.request.urlopen(url, timeout=60) as response:
            payload = response.read()
        actual = hashlib.sha256(payload).hexdigest()

        assert actual == sha, (
            f'recipe/meta.yaml pins {sha} for gedih3 {version}, but the sdist '
            f'PyPI serves hashes to {actual}. The recipe would fail to build. '
            f'This is what a stale hash from a previous release looks like.'
        )
