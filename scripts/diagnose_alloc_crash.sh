#!/usr/bin/env bash
# diagnose_alloc_crash.sh
#
# One-command post-mortem for Stage 7 allocation crashes (Plan 01-03 Task 2;
# OBS-02). Surfaces SLURM accounting evidence, optional `seff` summary,
# cgroup memory snapshots when present, and the most recent SENTINEL
# breadcrumbs from the per-region log artifacts. Designed to be the single
# command an operator runs after a crash to decide whether the failure was
# OOM-related and which logs to look at next.
#
# Usage:
#   scripts/diagnose_alloc_crash.sh [--job-id JOBID] [--logs-dir DIR] \
#                                   [--fixture-dir DIR]
#
# Flags:
#   --job-id ID        SLURM job id to query via sacct/seff. Defaults to
#                      $SLURM_JOB_ID when set.
#   --logs-dir DIR     Directory containing per-region worker logs. Defaults
#                      to ./logs at the project root.
#   --fixture-dir DIR  Test mode: read sacct/seff/region.log evidence from
#                      pre-canned text files inside DIR (sacct.txt,
#                      seff.txt, region.log). Used by automated verification
#                      so the script can be exercised without a live cluster.
#
# Exit codes:
#   0  Always — this script reports rather than judges. Operators read the
#      output and decide.
#
# Threat model (T-01-09): the script consumes external SLURM accounting
# output via quoted argv to commands and prints raw evidence; it never
# shell-evaluates cluster output.
set -euo pipefail

JOB_ID="${SLURM_JOB_ID:-}"
LOGS_DIR=""
FIXTURE_DIR=""

while [ $# -gt 0 ]; do
    case "$1" in
        --job-id)
            JOB_ID="$2"; shift 2 ;;
        --job-id=*)
            JOB_ID="${1#--job-id=}"; shift ;;
        --logs-dir)
            LOGS_DIR="$2"; shift 2 ;;
        --logs-dir=*)
            LOGS_DIR="${1#--logs-dir=}"; shift ;;
        --fixture-dir)
            FIXTURE_DIR="$2"; shift 2 ;;
        --fixture-dir=*)
            FIXTURE_DIR="${1#--fixture-dir=}"; shift ;;
        --help|-h)
            sed -n '1,40p' "$0"
            exit 0 ;;
        *)
            echo "Unknown argument: $1" >&2
            exit 2 ;;
    esac
done

# Resolve repo root (best effort) so the default LOGS_DIR is sensible.
if [ -z "$LOGS_DIR" ]; then
    SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
    REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
    LOGS_DIR="$REPO_ROOT/logs"
fi

echo "==========================================="
echo "Stage 7 allocation crash post-mortem"
echo "==========================================="
echo "Timestamp: $(date -u +"%Y-%m-%dT%H:%M:%SZ")"
echo "Job ID:    ${JOB_ID:-<unset>}"
echo "Logs dir:  $LOGS_DIR"
if [ -n "$FIXTURE_DIR" ]; then
    echo "Fixture dir: $FIXTURE_DIR"
fi
echo

# ---------------------------------------------------------------------------
# 1. SLURM accounting evidence (sacct / seff / cgroup) or fixture-mode reads.
# ---------------------------------------------------------------------------
echo "----- SLURM accounting (sacct) -----"
if [ -n "$FIXTURE_DIR" ] && [ -f "$FIXTURE_DIR/sacct.txt" ]; then
    cat "$FIXTURE_DIR/sacct.txt"
elif [ -n "$JOB_ID" ] && command -v sacct >/dev/null 2>&1; then
    # Quoted argv only — never shell-evaluate the JOB_ID value.
    sacct -j "$JOB_ID" \
        --format=JobIDRaw,JobName,State,ExitCode,MaxRSS,AveRSS,Elapsed \
        --units=M 2>&1 || true
else
    echo "(sacct skipped — no JOB_ID and no fixture)"
fi
echo

echo "----- SLURM job efficiency (seff, optional) -----"
if [ -n "$FIXTURE_DIR" ] && [ -f "$FIXTURE_DIR/seff.txt" ]; then
    cat "$FIXTURE_DIR/seff.txt"
elif [ -n "$JOB_ID" ] && command -v seff >/dev/null 2>&1; then
    seff "$JOB_ID" 2>&1 || true
else
    echo "(seff skipped — not installed or no JOB_ID)"
fi
echo

echo "----- cgroup memory snapshot -----"
if [ -n "$FIXTURE_DIR" ] && [ -f "$FIXTURE_DIR/cgroup.txt" ]; then
    cat "$FIXTURE_DIR/cgroup.txt"
elif [ -r /sys/fs/cgroup/memory.peak ]; then
    echo "memory.peak: $(cat /sys/fs/cgroup/memory.peak 2>/dev/null || true) bytes"
elif [ -r /sys/fs/cgroup/memory/memory.max_usage_in_bytes ]; then
    echo "memory.max_usage_in_bytes: $(cat /sys/fs/cgroup/memory/memory.max_usage_in_bytes 2>/dev/null || true) bytes"
else
    echo "(cgroup memory snapshot unavailable on this host)"
fi
echo

# ---------------------------------------------------------------------------
# 2. Sentinel records and most recent worker logs.
# ---------------------------------------------------------------------------
echo "----- Worker sentinels (most recent) -----"
SENTINEL_FILES=()
if [ -n "$FIXTURE_DIR" ]; then
    # Test mode: just look at fixture files.
    if [ -f "$FIXTURE_DIR/region.log" ]; then
        SENTINEL_FILES+=("$FIXTURE_DIR/region.log")
    fi
elif [ -d "$LOGS_DIR" ]; then
    # Look in worker_logs/* under each region work dir AND directly under
    # the central logs directory. Limit to recent files.
    while IFS= read -r f; do
        SENTINEL_FILES+=("$f")
    done < <(find "$LOGS_DIR" -type f \( -name '*.log' -o -name 'worker_*.log' \) -mtime -7 2>/dev/null | sort | tail -20)
fi

if [ "${#SENTINEL_FILES[@]}" -eq 0 ]; then
    echo "(no candidate log files found in $LOGS_DIR)"
else
    for f in "${SENTINEL_FILES[@]}"; do
        if grep -q 'SENTINEL\|"event":"SENTINEL"' "$f" 2>/dev/null; then
            echo "## $f"
            grep -E 'SENTINEL|"event":"SENTINEL"' "$f" | tail -5
            echo
        fi
    done
fi

# ---------------------------------------------------------------------------
# 3. Latest Dinamica subprocess log path (if any).
# ---------------------------------------------------------------------------
echo "----- Latest Dinamica subprocess log paths -----"
if [ -n "$FIXTURE_DIR" ] && [ -f "$FIXTURE_DIR/region.log" ]; then
    grep -E 'DINAMICA_LOG_PATH' "$FIXTURE_DIR/region.log" | tail -5 || \
        echo "(no DINAMICA_LOG_PATH lines in fixture region.log)"
elif [ -d "$LOGS_DIR" ]; then
    find "$LOGS_DIR" -type f -name '*.log' -mtime -7 2>/dev/null | \
        xargs -I{} grep -lE 'DINAMICA_LOG_PATH' {} 2>/dev/null | \
        head -10 || true
fi
echo

# ---------------------------------------------------------------------------
# 4. One-line crash summary from the most recent SENTINEL JSON line, if any.
# ---------------------------------------------------------------------------
echo "----- Crash summary -----"
LAST_SENTINEL=""
for f in "${SENTINEL_FILES[@]}"; do
    if [ -f "$f" ]; then
        candidate=$(grep -E '"event":"SENTINEL"' "$f" 2>/dev/null | tail -1 || true)
        if [ -n "$candidate" ]; then
            LAST_SENTINEL="$candidate"
        fi
    fi
done

if [ -n "$LAST_SENTINEL" ]; then
    echo "$LAST_SENTINEL"
else
    echo "(no JSON SENTINEL records found in scanned logs)"
fi
echo

echo "==========================================="
echo "Post-mortem complete."
echo "==========================================="
