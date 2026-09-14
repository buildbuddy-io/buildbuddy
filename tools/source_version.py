#!/usr/bin/env python3
"""Read the checkout's root VERSION, never Git tags.

The file must contain ASCII vMAJOR.MINOR.PATCH or vMAJOR.MINOR.PATCH-dev,
optionally followed by one LF or CRLF line ending. Each numeric component is
either 0 or starts with 1-9; leading zeroes and all other whitespace are rejected.

Usage: python3 tools/source_version.py [path/to/VERSION]
The default path is relative to this module, not the current working directory.
Explicit relative paths are resolved from the current working directory.
"""

import argparse
from pathlib import Path
import re
import sys


DEFAULT_VERSION_PATH = Path(__file__).resolve().parent.parent / "VERSION"
_VERSION_PATTERN = re.compile(
    rb"v(?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*)(?:-dev)?(?:\r?\n)?"
)


def read_version(path=None):
    """Return a validated version without its optional trailing line ending.

    Raises OSError if the file cannot be read, or ValueError for invalid content.
    No version is inferred when VERSION is absent or invalid.
    """
    path = DEFAULT_VERSION_PATH if path is None else Path(path)
    content = path.read_bytes()
    if _VERSION_PATTERN.fullmatch(content) is None:
        raise ValueError(
            f"{path}: expected vMAJOR.MINOR.PATCH or vMAJOR.MINOR.PATCH-dev "
            "with canonical ASCII numbers and at most one LF or CRLF line ending"
        )
    return content.rstrip(b"\r\n").decode("ascii")


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "path", nargs="?", help="VERSION file to read (default: checkout root VERSION)"
    )
    args = parser.parse_args(argv)
    try:
        version = read_version(args.path)
    except (OSError, ValueError) as error:
        print(f"source_version: {error}", file=sys.stderr)
        return 1
    print(version)
    return 0


if __name__ == "__main__":
    sys.exit(main())
