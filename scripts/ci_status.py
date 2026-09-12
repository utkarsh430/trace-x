"""Report GitHub Actions conclusions for a commit.

`P0.ci` is only PASS when CI has actually RUN AND PASSED -- "the workflows are
committed" is not evidence, and neither is "a run exists". This uses the public
REST API, so it needs no token; downloading job LOGS requires repo admin rights,
which is why CI failures must be diagnosed from reproducible local runs rather
than by reading the log.
"""

from __future__ import annotations

import json
import re
import shutil
import subprocess
import sys
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
GIT = shutil.which("git") or "git"
API = "https://api.github.com/repos/{repo}/actions/runs?head_sha={sha}"


def _git(*args: str) -> str:
    return subprocess.run(  # noqa: S603
        [GIT, *args], capture_output=True, text=True, cwd=ROOT, check=False
    ).stdout.strip()


# owner/repo only. The slug comes from `git remote get-url`, which is repo
# config and therefore attacker-influenceable, so it is validated before being
# interpolated into a URL.
SLUG_RE = re.compile(r"^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$")


def repo_slug() -> str | None:
    url = _git("remote", "get-url", "origin")
    if not url:
        return None
    slug = url.removesuffix(".git").removeprefix("git@github.com:")
    slug = slug.removeprefix("https://github.com/")
    if not SLUG_RE.match(slug):
        print(
            f"`origin` is not a github.com repository ({url!r}); cannot query Actions.",
            file=sys.stderr,
        )
        return None
    return slug


def main() -> int:
    sha = sys.argv[1] if len(sys.argv) > 1 else _git("rev-parse", "HEAD")
    repo = repo_slug()
    if not repo:
        print("No `origin` remote configured, so CI cannot have run.", file=sys.stderr)
        return 2

    if not re.fullmatch(r"[0-9a-fA-F]{7,40}", sha):
        print(f"refusing to query a non-commit-sha value: {sha!r}", file=sys.stderr)
        return 2

    url = API.format(repo=repo, sha=sha)
    # Defence in depth: never let a crafted remote turn this into a file:// read.
    if urllib.parse.urlparse(url).scheme != "https":
        print(f"refusing a non-https URL: {url!r}", file=sys.stderr)
        return 2

    try:
        # nosec B310 - the URL is a constant https template; the slug and sha are
        # regex-validated above and the scheme is asserted, so a file:/ or custom
        # scheme cannot reach this call.
        with urllib.request.urlopen(url, timeout=30) as resp:  # noqa: S310 # nosec B310
            runs = json.load(resp).get("workflow_runs", [])
    except (urllib.error.URLError, TimeoutError, json.JSONDecodeError) as exc:
        print(f"Could not reach the GitHub API: {exc}", file=sys.stderr)
        return 2

    print(f"{repo} @ {sha[:7]}")
    if not runs:
        print("  no workflow runs recorded for this commit", file=sys.stderr)
        return 1

    failed, pending = [], []
    for r in sorted(runs, key=lambda x: x["name"]):
        concl = r.get("conclusion") or r["status"]
        print(f"  {r['name']:<20} {concl}")
        if r["status"] != "completed":
            pending.append(r["name"])
        elif r["conclusion"] != "success":
            failed.append(r["name"])

    if pending:
        print(f"\nstill running: {', '.join(pending)}", file=sys.stderr)
        return 1
    if failed:
        print(f"\nFAILED: {', '.join(failed)}", file=sys.stderr)
        return 1
    print(f"\nAll {len(runs)} workflows passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
