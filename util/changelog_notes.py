#! /usr/bin/env python
"""Print the changelog.txt entry for a version, as one unwrapped paragraph.

Usage: changelog_notes.py VERSION [CHANGELOG]

Exits non-zero if the changelog has no entry for VERSION. Used by the release
workflow for GitHub Release notes, and by `make release` as a pre-flight check.
"""

from __future__ import print_function
import io
import os
import sys


def entry_for(version, changelog):
    prefix = "v%s - " % version
    lines = None
    with io.open(changelog, encoding="utf-8") as fh:
        for line in fh:
            line = line.rstrip()
            if lines is None:
                if line.startswith(prefix):
                    lines = [line[len(prefix):].strip()]
            elif line.strip():
                lines.append(line.strip())
            else:
                break
    return " ".join(lines) if lines else None


def main(argv):
    if len(argv) not in (2, 3):
        print(__doc__.strip(), file=sys.stderr)
        return 2
    version = argv[1].lstrip("v")
    changelog = argv[2] if len(argv) == 3 else os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        "changelog.txt")
    notes = entry_for(version, changelog)
    if not notes:
        print("Error: no entry for v%s in %s" % (version, changelog),
              file=sys.stderr)
        return 1
    print(notes)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
