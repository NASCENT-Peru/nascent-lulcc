#!/bin/bash
# preflight_rate_tables.sh
# Phase 3.6 capstone pre-flight gate (plan 03.6-05, D-11 / PIPE-08 / PIPE-09).
#
# HARD BLOCKING PREREQUISITE. Run this from the project root on a login / Rundeck
# node BEFORE submitting scripts/submit_allocation_scenario.sh. It is NOT an SBATCH
# job (it does not call sbatch). It refuses to let the live run launch unless:
#
#   1. A BAU-<region>-trans_rates-<year_ant>.csv exists for EVERY year_ant in the
#      chosen schedule, for EVERY region (the rate CSVs the allocation loop reads
#      one-per-year_ant at src/allocation.r:818-826 / 1937-1950 — a missing one
#      stop()s that timestep mid-run, after hours of compute), AND
#   2. (PIPE-09) the HPC working tree is sane: a clean `git status --porcelain`
#      (HARD block) plus a best-effort currency check — the stale-tree guard that
#      addresses the Phase 3.3 ALLOC-08 regression root cause (a behind/dirty
#      checkout silently running the old step_length=4 driver / pre-D-05 code).
#
# The demand table (scenario_area_mods_csv / simulation_lulc_areas_2060.csv) is
# NOT checked here: it is an UPSTREAM Stage-4 input (src/old/simulation_trans_tables_prep.r)
# already baked into the rate CSVs, not an allocation input, and the achieved 2060
# areas legitimately differ from it (what the model can place ≠ demand) — so it is
# neither a launch prerequisite nor a pass/fail criterion.
#
# year_ant set == head(simulation_year_steps, -1) == 2022,2024,2028,...,2056
# (the schedule the rate CSVs were generated against, src/simulation_trans_rates_prep.r).
#
# Rate-table path (resolved, NEVER hardcoded — mirrors src/allocation.r:818-826):
#   {trans_rate_table_dir}/simulation-lulc-areas-scalar-{scalar}x/{scenario}/{region}/{scenario}-{region}-trans_rates-{year_ant}.csv
#   scalar_str = sprintf("%.1f", selected_scalar)  -> e.g. "9.0"
#
# On a missing rate CSV OR a dirty/behind tree it prints a structured
# "BLOCK preflight ..." line, enumerates every missing (region, year_ant) pair,
# references the Stage-4 re-run remediation
# (scripts/submit_simulation_trans_rates_estimation.sh — Phase 3.4 precedent /
# PIPE-08), and exits non-zero. Only when EVERY (region, year_ant) CSV is present
# AND the tree is clean does it print "OK preflight ..." and exit 0. Remote-currency
# checks (fetch / upstream / behind) are best-effort and advisory on an offline node;
# set PREFLIGHT_SKIP_GIT=1 to bypass the git checks entirely.
#
# regions.json is empty in the repo checkout and authoritative only on HPC scratch
# (config[["reg_dir"]]); this gate fails closed if the probe returns zero regions.
#
# Usage (BAU now; scenario-parameterised for Phase 4 S2 reuse):
#   bash scripts/preflight_rate_tables.sh
#   bash scripts/preflight_rate_tables.sh BAU
#   ALLOC_SCENARIO=BAU bash scripts/preflight_rate_tables.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
export PROJECT_ROOT
cd "$PROJECT_ROOT" || { echo "ERROR: cannot cd to PROJECT_ROOT: $PROJECT_ROOT" >&2; exit 1; }

# Scenario: positional arg wins, else ALLOC_SCENARIO, else BAU.
SCENARIO="${1:-${ALLOC_SCENARIO:-BAU}}"

# Stage-4 re-run remediation, surfaced verbatim on any missing rate CSV (PIPE-08).
REMEDIATION="regenerate via scripts/submit_simulation_trans_rates_estimation.sh — Stage-4 re-run prerequisite, see Phase 3.4 / PIPE-08"

echo "========================================="
echo "Phase 3.6 allocation pre-flight gate"
echo "========================================="
echo "Scenario:     $SCENARIO"
echo "Project root: $PROJECT_ROOT"
echo

# --- Resolve an Rscript binary ----------------------------------------------
# Prefer the project's allocation_env (the way submit_* scripts do via
# hpc_common.sh), but fall back to a plain `Rscript` on PATH when the env is
# already active. Modelled on verify_allocation_run.sh:46-63.
RSCRIPT_BIN=""
if [ -f "$SCRIPT_DIR/hpc_common.sh" ]; then
    # shellcheck source=/dev/null
    if source "$SCRIPT_DIR/hpc_common.sh" 2>/dev/null \
        && [ -n "${ENV_BASE_PATH:-}" ] \
        && [ -x "$ENV_BASE_PATH/allocation_env/bin/Rscript" ]; then
        RSCRIPT_BIN="$ENV_BASE_PATH/allocation_env/bin/Rscript"
    fi
fi
if [ -z "$RSCRIPT_BIN" ]; then
    if command -v Rscript >/dev/null 2>&1; then
        RSCRIPT_BIN="$(command -v Rscript)"
    else
        echo "ERROR: no Rscript found (allocation_env not present and Rscript not on PATH)." >&2
        echo "       Activate the allocation_env or add Rscript to PATH, then re-run." >&2
        exit 2
    fi
fi
echo "Rscript:      $RSCRIPT_BIN"
echo

# --- Resolve region labels from regions.json (config[["reg_dir"]]) ----------
# Reuses the inline get_config() probe shape from submit_allocation_scenario.sh:79
# / submit_allocation_smoke.sh:70. Region labels come ONLY from this trusted probe
# (never free operator text) — T-036-04 mitigation.
readarray -t REGIONS < <("$RSCRIPT_BIN" --vanilla -e "
  setwd(Sys.getenv('PROJECT_ROOT')); source('src/setup.r'); cfg <- get_config();
  rj <- file.path(cfg[['reg_dir']], 'regions.json');
  if (!file.exists(rj)) quit(status = 0);
  regions <- jsonlite::fromJSON(rj);
  labs <- regions[['label']];
  labs <- labs[!is.na(labs) & nzchar(labs)];
  cat(labs, sep = '\n')
" 2>/dev/null)

# Fail closed (T-036-04): an empty region list means regions.json is missing/empty
# (the repo checkout) — the schedule is authoritative only on HPC scratch.
if [ "${#REGIONS[@]}" -eq 0 ]; then
    echo "BLOCK preflight scenario=$SCENARIO reason=no_regions missing=0" >&2
    echo "ERROR: region probe returned zero regions." >&2
    echo "       regions.json under config[['reg_dir']] is missing or empty." >&2
    echo "       It is authoritative only on HPC scratch — run this gate there." >&2
    exit 1
fi

# --- Resolve schedule + rate-table dir + scalar -----------------------------
# trans_rate_table_dir is config-derived (outputs/transition_tables resolved
# under data_basepath); scalar_str = sprintf("%.1f", selected_scalar) exactly as
# src/allocation.r:817/1937; simulation_year_steps is the authoritative schedule
# (NOT the step_length seq()) — D-11 / RUN-02.
readarray -t PROBE < <("$RSCRIPT_BIN" --vanilla -e "
  setwd(Sys.getenv('PROJECT_ROOT')); source('src/setup.r'); cfg <- get_config();
  cat(cfg[['trans_rate_table_dir']], '\n');
  cat(sprintf('%.1f', cfg[['selected_scalar']]), '\n');
  cat(paste(cfg[['simulation_year_steps']], collapse = ' '), '\n')
" 2>/dev/null)
RATE_DIR="$(echo "${PROBE[0]:-}" | xargs)"
SCALAR_STR="$(echo "${PROBE[1]:-}" | xargs)"
read -r -a YEAR_STEPS <<< "${PROBE[2]:-}"

if [ -z "$RATE_DIR" ] || [ -z "$SCALAR_STR" ] || [ "${#YEAR_STEPS[@]}" -lt 2 ]; then
    echo "BLOCK preflight scenario=$SCENARIO reason=config_probe_failed missing=0" >&2
    echo "ERROR: could not resolve trans_rate_table_dir / selected_scalar / simulation_year_steps from get_config()." >&2
    exit 1
fi

# year_ant set == head(simulation_year_steps, -1): every step's anterior year, the
# years the rate CSVs are keyed on (2022,2024,...,2056). The last boundary (2060)
# is only ever a posterior year, so it never has its own rate CSV.
YEAR_ANT=("${YEAR_STEPS[@]:0:${#YEAR_STEPS[@]}-1}")

SCALAR_SEGMENT="simulation-lulc-areas-scalar-${SCALAR_STR}x"

echo "Regions (${#REGIONS[@]}):       ${REGIONS[*]}"
echo "Schedule (${#YEAR_STEPS[@]} boundaries): ${YEAR_STEPS[*]}"
echo "year_ant set (${#YEAR_ANT[@]}):    ${YEAR_ANT[*]}"
echo "Rate-table dir:      $RATE_DIR"
echo "Scalar segment:      $SCALAR_SEGMENT"
echo

# --- Check every (region, year_ant) rate CSV --------------------------------
declare -a MISSING_PAIRS=()
CHECKED=0
for REGION in "${REGIONS[@]}"; do
    for YA in "${YEAR_ANT[@]}"; do
        CHECKED=$((CHECKED + 1))
        CSV="$RATE_DIR/$SCALAR_SEGMENT/$SCENARIO/$REGION/$SCENARIO-$REGION-trans_rates-$YA.csv"
        if [ ! -f "$CSV" ]; then
            MISSING_PAIRS+=("region=$REGION year_ant=$YA path=$CSV")
        fi
    done
done

# --- PIPE-09: git tree sanity (ZALF-adapted) --------------------------------
# A behind/dirty HPC checkout is the Phase 3.3 ALLOC-08 root cause (T-036-13):
# stale code silently runs the wrong schedule. On ZALF compute nodes the remote is
# often unreachable (no internet) and the working branch may not track an upstream,
# so currency-vs-remote cannot always be PROVEN here. Policy:
#   - HARD BLOCK only on a dirty working tree (locally verifiable; uncommitted
#     edits mean the running code is an unknown state), or a provably-behind tree.
#   - WARN (do not block) when fetch fails / there is no upstream — and always
#     print the current HEAD so the operator can confirm it is the intended head
#     (pull on the login node BEFORE allocating, since the node can't fetch).
#   - Set PREFLIGHT_SKIP_GIT=1 to bypass the git checks entirely.
GIT_BLOCK_REASONS=()
GIT_WARNINGS=()
if [ "${PREFLIGHT_SKIP_GIT:-0}" = "1" ]; then
    GIT_WARNINGS+=("git_check_skipped_by_PREFLIGHT_SKIP_GIT")
else
    # Dirty working tree is the one always-enforceable hard block.
    if [ -n "$(git status --porcelain 2>/dev/null)" ]; then
        GIT_BLOCK_REASONS+=("dirty_working_tree")
    fi
    # Best-effort fetch; a failure on an offline node is advisory, not a block.
    if ! git fetch --quiet 2>/dev/null; then
        GIT_WARNINGS+=("git_fetch_failed_could_not_verify_currency")
    fi
    # Behind upstream only matters if a tracking branch exists; otherwise advisory.
    UPSTREAM="$(git rev-parse --abbrev-ref --symbolic-full-name '@{upstream}' 2>/dev/null || true)"
    if [ -z "$UPSTREAM" ]; then
        GIT_WARNINGS+=("no_upstream_tracking_branch")
    else
        BEHIND="$(git rev-list --count 'HEAD..@{upstream}' 2>/dev/null || echo 0)"
        if [ "${BEHIND:-0}" -gt 0 ]; then
            GIT_BLOCK_REASONS+=("behind_upstream_by_${BEHIND}")
        fi
    fi
fi

# Always surface the current HEAD so the operator can eyeball the intended head.
GIT_BRANCH="$(git rev-parse --abbrev-ref HEAD 2>/dev/null || echo '?')"
GIT_HEAD="$(git rev-parse --short HEAD 2>/dev/null || echo '?')"
GIT_HEAD_SUBJECT="$(git log -1 --pretty=%s 2>/dev/null || echo '?')"
echo "Git HEAD:     $GIT_BRANCH @ $GIT_HEAD — $GIT_HEAD_SUBJECT"
if [ "${#GIT_WARNINGS[@]}" -gt 0 ]; then
    echo "Git WARN:     ${GIT_WARNINGS[*]}"
    echo "              (could not verify currency vs remote — confirm you pulled the intended head on the login node before allocating)"
fi
echo

# --- Verdict ----------------------------------------------------------------
N_MISSING=${#MISSING_PAIRS[@]}
GIT_BLOCKED=0
[ "${#GIT_BLOCK_REASONS[@]}" -gt 0 ] && GIT_BLOCKED=1

if [ "$N_MISSING" -gt 0 ] || [ "$GIT_BLOCKED" -eq 1 ]; then
    REASONS=""
    [ "$N_MISSING" -gt 0 ] && REASONS="${REASONS:+$REASONS,}missing_rate_tables"
    if [ "$GIT_BLOCKED" -eq 1 ]; then
        for gr in "${GIT_BLOCK_REASONS[@]}"; do
            REASONS="${REASONS:+$REASONS,}$gr"
        done
    fi

    echo "BLOCK preflight scenario=$SCENARIO reason=${REASONS} missing=${N_MISSING} regions=${#REGIONS[@]} year_ant=${#YEAR_ANT[@]} checked=${CHECKED}" >&2
    echo >&2

    if [ "$N_MISSING" -gt 0 ]; then
        echo "Missing rate tables (${N_MISSING} of ${CHECKED} (region, year_ant) pairs):" >&2
        for pair in "${MISSING_PAIRS[@]}"; do
            echo "  - $pair" >&2
        done
        echo >&2
        echo "REMEDIATION: $REMEDIATION" >&2
        echo "             If tables exist only for 2022 (the single-timestep state so far)," >&2
        echo "             every later year_ant needs its CSV first — a Stage-4 re-run." >&2
        echo >&2
    fi

    if [ "$GIT_BLOCKED" -eq 1 ]; then
        echo "PIPE-09 git-tree check FAILED: ${GIT_BLOCK_REASONS[*]}" >&2
        echo "             Commit or stash local changes so 'git status' is clean (and, if" >&2
        echo "             behind, 'git pull') before submitting. A stale/dirty tree is the" >&2
        echo "             Phase 3.3 ALLOC-08 regression root cause (T-036-13)." >&2
        echo "             To bypass intentionally: PREFLIGHT_SKIP_GIT=1 bash scripts/preflight_rate_tables.sh $SCENARIO" >&2
        echo >&2
    fi

    echo "DO NOT LAUNCH. Resolve the above, then re-run this gate until it prints 'OK preflight ...'." >&2
    exit 1
fi

echo "OK preflight scenario=$SCENARIO schedule=simulation_year_steps regions=${#REGIONS[@]} year_ant=${#YEAR_ANT[@]} checked=${CHECKED} all_rate_tables_present git_tree_ok"
echo
echo "Pre-flight passed. Safe to submit: bash scripts/submit_allocation_scenario.sh"
exit 0
