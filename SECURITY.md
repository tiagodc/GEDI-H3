# Security Policy

## Reporting a Vulnerability

Please do **not** open a public issue for a security vulnerability.

Report it privately through
[GitHub's private vulnerability reporting](https://github.com/tiagodc/GEDI-H3/security/advisories/new),
which is the preferred channel. If that is unavailable to you, email the
maintainers via the contact listed in [`CITATION.cff`](CITATION.cff).

Please include:

- the gedih3 version, Python version, and operating system
- what an attacker could achieve
- a minimal reproduction, if you have one

You can expect an acknowledgement within a few working days. We will let you
know whether we consider the report in scope, and coordinate disclosure timing
with you before publishing a fix.

## Supported Versions

Fixes are issued against the latest released version on PyPI and conda-forge.
There are no long-term support branches — please upgrade before reporting.

## Credentials and This Package

gedih3 authenticates to NASA Earthdata, and may be configured with S3 or other
remote-storage credentials. A few things worth knowing:

- **Earthdata credentials live in `~/.netrc`**, written by `earthaccess.login()`.
  gedih3 parses it (`GEDIAccessor.login`) only to check that an entry for
  `urs.earthdata.nasa.gov` exists and to fail with a useful message if it does
  not; the credentials themselves are used by `earthaccess` and the underlying
  HTTP stack. Keep the file mode `600`.
- **Remote-storage credentials** passed via `--s3-key` / `--s3-secret` or
  `configure_storage()` are held in process memory. Prefer `--s3-profile` and
  the standard AWS credential chain, or `--s3-anon` for public buckets, so
  secrets never appear in a shell history or a process listing.
- **`~/.gedih3.env`** may contain paths and, if you put them there, credentials.
  Treat it as sensitive.
- **Build and doctor logs** record file paths and granule identifiers. They do
  not record credentials, but review them before attaching one to a public
  issue.

If you find a case where gedih3 itself writes a credential to disk, into a log,
or into a report, that is a vulnerability — please report it through the channel
above.
