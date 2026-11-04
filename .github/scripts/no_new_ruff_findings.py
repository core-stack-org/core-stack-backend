"""Fail if the pull request adds ruff findings that its target branch doesn't have.

Usage: no_new_ruff_findings.py <ruff> <base dir> <head dir>

Only rules that point at real bugs are checked: syntax errors, repeated dict
keys, redefinitions and undefined names. Findings are counted per file and
rule, so moving code around doesn't count as new.
"""

import json
import os
import subprocess
import sys
from collections import Counter

RULES = "E9,F63,F7,F82,F601,F811"


def findings(ruff, directory):
    result = subprocess.run(
        [
            ruff,
            "check",
            "--select",
            RULES,
            "--exclude",
            "*.ipynb",
            "--output-format",
            "json",
            "--no-cache",
            ".",
        ],
        cwd=directory,
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode not in (0, 1):
        sys.exit(f"ruff failed in {directory}:\n{result.stderr}")
    return json.loads(result.stdout or "[]")


def key(finding, directory):
    path = os.path.relpath(finding["filename"], os.path.abspath(directory))
    return path, finding["code"]


def main():
    ruff, base, head = sys.argv[1:4]
    base_counts = Counter(key(f, base) for f in findings(ruff, base))
    head_findings = findings(ruff, head)
    head_counts = Counter(key(f, head) for f in head_findings)

    added = {k for k, n in head_counts.items() if n > base_counts[k]}
    if not added:
        print(
            f"No new findings for {RULES} "
            f"({sum(head_counts.values())} already on the target branch)."
        )
        return

    print(f"This pull request adds ruff findings ({RULES}):")
    for f in head_findings:
        path, code = key(f, head)
        if (path, code) in added:
            row = f["location"]["row"]
            print(f"  {path}:{row}: {code} {f['message']}")
    sys.exit(1)


if __name__ == "__main__":
    main()
