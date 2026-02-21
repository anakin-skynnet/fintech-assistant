#!/usr/bin/env python3
"""
Remove duplicate Getnet Closure jobs from the workspace, keeping the latest (by created_time).
Use when the bundle created duplicate job entries with the same display name.

Usage:
  python scripts/deduplicate_bundle_jobs.py [--dry-run] [--profile PROFILE]
  --dry-run   Only print what would be deleted; do not delete.
  --profile   Databricks CLI profile (optional).
"""
import argparse
import json
import subprocess
import sys


def run_cli(args: list[str], profile: str | None = None) -> str:
    cmd = ["databricks", "jobs", "list", "--limit", "100", "-o", "json"]
    if profile:
        cmd.extend(["-p", profile])
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
    if result.returncode != 0:
        print(result.stderr or result.stdout, file=sys.stderr)
        sys.exit(1)
    return result.stdout


def delete_job(job_id: int, profile: str | None = None) -> bool:
    cmd = ["databricks", "jobs", "delete", str(job_id)]
    if profile:
        cmd.extend(["-p", profile])
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
    if result.returncode != 0:
        print(f"  Failed to delete job_id={job_id}: {result.stderr or result.stdout}", file=sys.stderr)
        return False
    return True


def main():
    parser = argparse.ArgumentParser(description="Deduplicate Getnet Closure jobs (keep latest).")
    parser.add_argument("--dry-run", action="store_true", help="Only print; do not delete.")
    parser.add_argument("--profile", default=None, help="Databricks CLI profile.")
    args = parser.parse_args()

    raw = run_cli([], profile=args.profile)
    jobs = json.loads(raw)
    if not isinstance(jobs, list):
        jobs = jobs.get("jobs", jobs) if isinstance(jobs, dict) else []

    by_name: dict[str, list[dict]] = {}
    for j in jobs:
        name = (j.get("settings") or {}).get("name") or j.get("name") or ""
        if not name or "Getnet Closure" not in name:
            continue
        by_name.setdefault(name, []).append(j)

    to_delete = []
    for name, group in by_name.items():
        if len(group) <= 1:
            continue
        # Sort by created_time descending; keep first (latest), mark rest for deletion
        group.sort(key=lambda x: x.get("created_time") or 0, reverse=True)
        for dup in group[1:]:
            job_id = dup.get("job_id")
            if job_id is not None:
                to_delete.append((name, job_id, dup.get("created_time")))

    if not to_delete:
        print("No duplicate Getnet Closure jobs found.")
        return

    print(f"Found {len(to_delete)} duplicate job(s) to remove (keeping latest per name):\n")
    for name, job_id, ctime in to_delete:
        print(f"  {name!r}  job_id={job_id}  created_time={ctime}")

    if args.dry_run:
        print("\n[DRY-RUN] No jobs were deleted. Run without --dry-run to delete.")
        return

    print("\nDeleting...")
    ok = 0
    for _name, job_id, _ctime in to_delete:
        if delete_job(job_id, profile=args.profile):
            ok += 1
            print(f"  Deleted job_id={job_id}")
    print(f"Done. Deleted {ok}/{len(to_delete)} jobs.")


if __name__ == "__main__":
    main()
