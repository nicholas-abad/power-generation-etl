"""Remove secrets from files before a workflow uploads them as artifacts.

This repository is public, so anyone signed in to GitHub can download its
workflow artifacts. GitHub masks secrets in job logs, but not in uploaded
files. The ENTSO-E extractor writes failed request URLs, including
``securityToken=<key>``, into its failure manifest and log; those files were
uploaded unredacted until November 2026.

Every file under the given paths is rewritten in place with:

- the value of each environment variable in ``SECRET_ENV_VARS`` that is set,
  wherever it occurs, and
- the value of any ``securityToken=``, ``api_key=``, ``apikey=`` or ``token=``
  URL parameter, even if its key is not in the environment,

replaced by ``***``. Example: ``...?securityToken=abc123&documentType=A73``
becomes ``...?securityToken=***&documentType=A73``.

Usage (run before ``actions/upload-artifact``, with the job's secrets in env):
    python3 src/redact_artifacts.py extracted_data output

Standard library only, so it runs before ``uv sync``. Never prints a secret.
Exit codes: 0 done (also when a path is missing), 1 a file could not be rewritten.
"""

import os
import re
import sys
from pathlib import Path

SECRET_ENV_VARS = (
    "ENTSOE_API_KEY",
    "OPENELECTRICITY_API_KEY",
    "CL_API_KEY",
    "EIA_API_KEY",
    "POSTGRES_PASSWORD",
    "PGPASSWORD",
    "DATABASE_URL",
    "DIRECT_DATABASE_URL",
)
# Shorter values would also replace ordinary text that happens to match.
MIN_SECRET_LENGTH = 8
MASK = b"***"
URL_PARAMETER = re.compile(
    rb"((?:securityToken|api_key|apikey|token)=)[^&\s\"'<>]+", re.IGNORECASE
)


def secret_values(environ: dict[str, str]) -> list[bytes]:
    """Values of the set secret variables, longest first."""
    values = {
        environ[name].encode()
        for name in SECRET_ENV_VARS
        if len(environ.get(name, "")) >= MIN_SECRET_LENGTH
    }
    return sorted(values, key=len, reverse=True)


def redact(content: bytes, secrets: list[bytes]) -> bytes:
    """Content with every secret value and token URL parameter masked."""
    for secret in secrets:
        content = content.replace(secret, MASK)
    return URL_PARAMETER.sub(rb"\1" + MASK, content)


def redact_paths(paths: list[Path], secrets: list[bytes]) -> int:
    """Rewrite every file under ``paths``; return how many files changed."""
    changed = 0
    for root in paths:
        files = [root] if root.is_file() else sorted(root.rglob("*"))
        for path in files:
            if not path.is_file():
                continue
            original = path.read_bytes()
            cleaned = redact(original, secrets)
            if cleaned != original:
                path.write_bytes(cleaned)
                changed += 1
    return changed


def main(argv: list[str]) -> int:
    if not argv:
        print("usage: redact_artifacts.py <path> [<path> ...]", file=sys.stderr)
        return 1
    secrets = secret_values(dict(os.environ))
    try:
        changed = redact_paths([Path(p) for p in argv], secrets)
    except OSError as error:
        print(f"redaction failed: {type(error).__name__}", file=sys.stderr)
        return 1
    print(
        f"redacted {changed} file(s) under {len(argv)} path(s); "
        f"{len(secrets)} secret value(s) from the environment"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
