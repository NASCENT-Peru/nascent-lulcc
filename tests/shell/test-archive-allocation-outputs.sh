#!/usr/bin/env bash
# tests/shell/test-archive-allocation-outputs.sh
#
# Fixture test for scripts/archive_allocation_outputs.sh in library mode.
#
# Pure bash: no R, micromamba, SLURM or raster libraries. The script is sourced
# (which defines its functions without running main), the job-listing function
# is replaced by a stub per case, and every case runs against a throwaway
# scratch tree of empty files standing in for .tif outputs. main and
# resolve_sim_out are never called (they need micromamba and R); only the
# argument-parsing paths of the script are exercised as a subprocess.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
SCRIPT="$REPO_ROOT/scripts/archive_allocation_outputs.sh"
PASS=0
FAIL=0

FIXTURE_ROOT="$(mktemp -d)"
trap 'rm -rf "$FIXTURE_ROOT"' EXIT

pass() { echo "PASS: $1"; PASS=$((PASS+1)); }
fail() { echo "FAIL: $1"; [ -n "${2:-}" ] && printf '  %s\n' "$2"; FAIL=$((FAIL+1)); }

# check <name> <command...>: PASS when the command succeeds.
check() {
    local name="$1"; shift
    if "$@"; then pass "$name"; else fail "$name"; fi
}

# refuses <name> <command...>: PASS when the command fails (non-zero).
refuses() {
    local name="$1"; shift
    local rc=0
    "$@" >/dev/null 2>&1 || rc=$?
    if [ "$rc" -ne 0 ]; then pass "$name"; else fail "$name" "expected non-zero exit, got 0"; fi
}

# Sorted recursive listing relative to a directory (identity check for trees).
snapshot() { (cd "$1" && find . | LC_ALL=C sort); }

# make_fixture: print a fresh fake scratch root containing outputs/simulations
# with NAT (2 years x 2 regions posteriors + 2 national mosaics + run manifest),
# CUL (1 posterior), BAU and SOC absent, and one unrelated top-level entry.
make_fixture() {
    local root so y r
    root="$(mktemp -d "$FIXTURE_ROOT/scratch.XXXXXX")"
    so="$root/outputs/simulations"
    mkdir -p "$so"
    for y in 2024 2026; do
        for r in costa_peruana andes; do
            mkdir -p "$so/NAT/$y/region_$r/worker_logs"
            : > "$so/NAT/$y/region_$r/posterior.tif"
            : > "$so/NAT/$y/region_$r/anterior.tif"
            : > "$so/NAT/$y/region_$r/trans_rates.csv"
        done
        : > "$so/NAT/$y/posterior_$y.tif"
    done
    : > "$so/NAT/run_manifest.csv"
    mkdir -p "$so/CUL/2024/region_costa_peruana"
    : > "$so/CUL/2024/region_costa_peruana/posterior.tif"
    mkdir -p "$so/unrelated_dir"
    : > "$so/unrelated_dir/keep.txt"
    echo "$root"
}

# ---------------------------------------------------------------------------
# Library mode: sourcing defines functions and does not run main
# ---------------------------------------------------------------------------
if [ ! -f "$SCRIPT" ]; then
    fail "script_exists" "missing: $SCRIPT"
    echo "============================="
    echo "PASS: $PASS  FAIL: $FAIL"
    exit 1
fi

SOURCE_OUT="$( (source "$SCRIPT") 2>&1 )" || true
if grep -q "STATE archive_allocation_outputs" <<<"$SOURCE_OUT"; then
    fail "library_mode_does_not_run_main" "$SOURCE_OUT"
else
    pass "library_mode_does_not_run_main"
fi

# shellcheck source=/dev/null
source "$SCRIPT"

for fn in validate_sim_out validate_tag validate_scenarios running_allocation_jobs \
          report_scenario run_archive resolve_sim_out main; do
    if declare -F "$fn" >/dev/null; then
        pass "defines_function_$fn"
    else
        fail "defines_function_$fn"
    fi
done

# Default stub: no matching jobs queued or running.
running_allocation_jobs() { :; }

# ---------------------------------------------------------------------------
# validate_sim_out
# ---------------------------------------------------------------------------
ROOT_V="$(make_fixture)"
SO_V="$ROOT_V/outputs/simulations"
mkdir -p "$FIXTURE_ROOT/elsewhere"
mkdir -p "${ROOT_V}2/outputs/simulations"

export HPC_SCRATCH_ROOT="$ROOT_V"
refuses "validate_sim_out_rejects_empty"            validate_sim_out ""
refuses "validate_sim_out_rejects_root_slash"       validate_sim_out "/"
refuses "validate_sim_out_rejects_relative"         validate_sim_out "outputs/simulations"
refuses "validate_sim_out_rejects_missing_dir"      validate_sim_out "$ROOT_V/outputs/does_not_exist"
refuses "validate_sim_out_rejects_regular_file"     validate_sim_out "$SO_V/unrelated_dir/keep.txt"
refuses "validate_sim_out_rejects_outside_scratch"  validate_sim_out "$FIXTURE_ROOT/elsewhere"
refuses "validate_sim_out_rejects_sibling_prefix"   validate_sim_out "${ROOT_V}2/outputs/simulations"
refuses "validate_sim_out_rejects_scratch_root_itself" validate_sim_out "$ROOT_V"
refuses "validate_sim_out_rejects_dotdot"           validate_sim_out "$ROOT_V/outputs/../outputs/simulations"
check   "validate_sim_out_accepts_scratch_simulations" validate_sim_out "$SO_V"
check   "validate_sim_out_accepts_trailing_slash"   validate_sim_out "$SO_V/"

HPC_SCRATCH_ROOT="" refuses "validate_sim_out_rejects_empty_scratch_root" validate_sim_out "$SO_V"
HPC_SCRATCH_ROOT="/" refuses "validate_sim_out_rejects_slash_scratch_root" validate_sim_out "$SO_V"
unset HPC_SCRATCH_ROOT
refuses "validate_sim_out_rejects_unset_scratch_root" validate_sim_out "$SO_V"
export HPC_SCRATCH_ROOT="$ROOT_V"

# ---------------------------------------------------------------------------
# validate_tag / validate_scenarios
# ---------------------------------------------------------------------------
refuses "validate_tag_rejects_slash"     validate_tag "a/b"
refuses "validate_tag_rejects_dotdot"    validate_tag ".."
refuses "validate_tag_rejects_inner_dotdot" validate_tag "x..y"
refuses "validate_tag_rejects_empty"     validate_tag ""
refuses "validate_tag_rejects_space"     validate_tag "a b"
check   "validate_tag_accepts_phase_tag" validate_tag "pre_interventions_phase5"
check   "validate_tag_accepts_dated_tag" validate_tag "pre_interventions_20261014"

refuses "validate_scenarios_rejects_unknown"   validate_scenarios NAT FOO
refuses "validate_scenarios_rejects_path"      validate_scenarios "../NAT"
refuses "validate_scenarios_rejects_lowercase" validate_scenarios nat
refuses "validate_scenarios_rejects_empty"     validate_scenarios
refuses "validate_scenarios_rejects_duplicate" validate_scenarios NAT NAT
check   "validate_scenarios_accepts_subset"    validate_scenarios NAT CUL
check   "validate_scenarios_accepts_all_four"  validate_scenarios BAU NAT CUL SOC

# ---------------------------------------------------------------------------
# Dry run: reports counts, changes nothing
# ---------------------------------------------------------------------------
ROOT_D="$(make_fixture)"
SO_D="$ROOT_D/outputs/simulations"
export HPC_SCRATCH_ROOT="$ROOT_D"
BEFORE_D="$(snapshot "$SO_D")"
RC=0
OUT_D="$(run_archive "$SO_D" dry-run tagD BAU NAT CUL SOC 2>&1)" || RC=$?
AFTER_D="$(snapshot "$SO_D")"

check "dry_run_exit_zero" test "$RC" -eq 0
check "dry_run_changes_nothing" test "$BEFORE_D" = "$AFTER_D"
for pat in \
    "[NAT] region posteriors: 4" \
    "[NAT] national mosaics: 2" \
    "[NAT] posterior years: 2024 2026" \
    "[CUL] region posteriors: 1" \
    "[CUL] national mosaics: 0" \
    "[BAU] absent - nothing to move" \
    "[SOC] absent - nothing to move" \
    "left in place: unrelated_dir" \
    "[NAT] would move: $SO_D/NAT -> $SO_D/_archive_tagD/NAT" \
    "[CUL] would move: $SO_D/CUL -> $SO_D/_archive_tagD/CUL" \
    "STATE archive_allocation_outputs mode=dry-run sim_out=$SO_D archive=$SO_D/_archive_tagD moved=0"; do
    if grep -qF -- "$pat" <<<"$OUT_D"; then
        pass "dry_run_prints: $pat"
    else
        fail "dry_run_prints: $pat" "$OUT_D"
    fi
done
if grep -E '^[[:space:]]*2 region_andes$' <<<"$OUT_D" >/dev/null \
   && grep -E '^[[:space:]]*2 region_costa_peruana$' <<<"$OUT_D" >/dev/null; then
    pass "dry_run_per_region_breakdown"
else
    fail "dry_run_per_region_breakdown" "$OUT_D"
fi
check "dry_run_creates_no_archive_dir" test ! -e "$SO_D/_archive_tagD"

# Dry run with a matching job only warns.
running_allocation_jobs() { echo "123 alloc-region RUNNING"; }
RC=0
OUT_DJ="$(run_archive "$SO_D" dry-run tagD NAT CUL 2>&1)" || RC=$?
check "dry_run_with_running_job_exit_zero" test "$RC" -eq 0
if grep -q "WARNING" <<<"$OUT_DJ" && grep -qF "123 alloc-region RUNNING" <<<"$OUT_DJ"; then
    pass "dry_run_with_running_job_warns"
else
    fail "dry_run_with_running_job_warns" "$OUT_DJ"
fi
check "dry_run_with_running_job_changes_nothing" test "$BEFORE_D" = "$(snapshot "$SO_D")"
running_allocation_jobs() { :; }

# ---------------------------------------------------------------------------
# Apply: renames NAT and CUL out of the launcher pattern
# ---------------------------------------------------------------------------
ROOT_A="$(make_fixture)"
SO_A="$ROOT_A/outputs/simulations"
export HPC_SCRATCH_ROOT="$ROOT_A"
NAT_BEFORE="$(snapshot "$SO_A/NAT")"
CUL_BEFORE="$(snapshot "$SO_A/CUL")"
UNREL_BEFORE="$(snapshot "$SO_A/unrelated_dir")"
RC=0
OUT_A="$(run_archive "$SO_A" apply tagA BAU NAT CUL SOC 2>&1)" || RC=$?
ARCH_A="$SO_A/_archive_tagA"

check "apply_exit_zero" test "$RC" -eq 0
check "apply_nat_source_gone" test ! -e "$SO_A/NAT"
check "apply_cul_source_gone" test ! -e "$SO_A/CUL"
check "apply_nat_archived_identical" test "$NAT_BEFORE" = "$(snapshot "$ARCH_A/NAT")"
check "apply_cul_archived_identical" test "$CUL_BEFORE" = "$(snapshot "$ARCH_A/CUL")"
check "apply_unrelated_untouched" test "$UNREL_BEFORE" = "$(snapshot "$SO_A/unrelated_dir")"
check "apply_absent_scenarios_not_created" test ! -e "$SO_A/BAU" -a ! -e "$SO_A/SOC" \
    -a ! -e "$ARCH_A/BAU" -a ! -e "$ARCH_A/SOC"

LEFT=0
for s in BAU NAT CUL SOC; do
    for f in "$SO_A/$s"/*/region_*/posterior.tif; do
        [ -e "$f" ] && LEFT=$((LEFT+1))
    done
done
check "apply_no_launcher_pattern_posteriors_left" test "$LEFT" -eq 0

MANIFEST="$ARCH_A/ARCHIVE_MANIFEST.tsv"
check "apply_manifest_exists" test -f "$MANIFEST"
if [ -f "$MANIFEST" ]; then
    check "apply_manifest_header_plus_two_rows" test "$(wc -l < "$MANIFEST")" -eq 3
    check "apply_manifest_header_once" test "$(grep -c '^timestamp' "$MANIFEST")" -eq 1
    NAT_ROW="$(awk -F'\t' '$5 == "NAT"' "$MANIFEST")"
    CUL_ROW="$(awk -F'\t' '$5 == "CUL"' "$MANIFEST")"
    check "apply_manifest_nat_row_counts" test "$(awk -F'\t' '{print $6"|"$7"|"$8"|"$9}' <<<"$NAT_ROW")" \
        = "$SO_A/NAT|$ARCH_A/NAT|4|2"
    check "apply_manifest_cul_row_counts" test "$(awk -F'\t' '{print $6"|"$7"|"$8"|"$9}' <<<"$CUL_ROW")" \
        = "$SO_A/CUL|$ARCH_A/CUL|1|0"
    check "apply_manifest_rows_have_nine_fields" test \
        "$(awk -F'\t' 'NF != 9' "$MANIFEST" | wc -l)" -eq 0
fi
if grep -qF "STATE archive_allocation_outputs mode=apply sim_out=$SO_A archive=$ARCH_A moved=2" <<<"$OUT_A"; then
    pass "apply_state_line"
else
    fail "apply_state_line" "$OUT_A"
fi

# Re-running apply with the same tag is a no-op (everything already archived).
RC=0
OUT_A2="$(run_archive "$SO_A" apply tagA BAU NAT CUL SOC 2>&1)" || RC=$?
check "apply_rerun_exit_zero" test "$RC" -eq 0
check "apply_rerun_moves_nothing" grep -qF "moved=0" <<<"$OUT_A2"
check "apply_rerun_manifest_unchanged" test "$(wc -l < "$MANIFEST")" -eq 3

# ---------------------------------------------------------------------------
# Apply refuses when an archive target already exists (nothing moved)
# ---------------------------------------------------------------------------
ROOT_C="$(make_fixture)"
SO_C="$ROOT_C/outputs/simulations"
export HPC_SCRATCH_ROOT="$ROOT_C"
mkdir -p "$SO_C/_archive_tagC/CUL"
BEFORE_C="$(snapshot "$SO_C")"
RC=0
OUT_C="$(run_archive "$SO_C" apply tagC BAU NAT CUL SOC 2>&1)" || RC=$?
check "apply_existing_target_refuses" test "$RC" -ne 0
check "apply_existing_target_moves_nothing" test "$BEFORE_C" = "$(snapshot "$SO_C")"
check "apply_existing_target_names_conflict" grep -qF "$SO_C/_archive_tagC/CUL" <<<"$OUT_C"

# ---------------------------------------------------------------------------
# Apply refuses when an allocation job is queued or running (nothing moved)
# ---------------------------------------------------------------------------
ROOT_J="$(make_fixture)"
SO_J="$ROOT_J/outputs/simulations"
export HPC_SCRATCH_ROOT="$ROOT_J"
running_allocation_jobs() { echo "841999 lulc-allocation-smoke PENDING"; }
BEFORE_J="$(snapshot "$SO_J")"
RC=0
OUT_J="$(run_archive "$SO_J" apply tagJ BAU NAT CUL SOC 2>&1)" || RC=$?
check "apply_running_job_refuses" test "$RC" -ne 0
check "apply_running_job_moves_nothing" test "$BEFORE_J" = "$(snapshot "$SO_J")"
check "apply_running_job_named" grep -qF "841999 lulc-allocation-smoke PENDING" <<<"$OUT_J"
running_allocation_jobs() { :; }

# ---------------------------------------------------------------------------
# run_archive re-validates its inputs (defence in depth)
# ---------------------------------------------------------------------------
ROOT_R="$(make_fixture)"
SO_R="$ROOT_R/outputs/simulations"
export HPC_SCRATCH_ROOT="$ROOT_R"
BEFORE_R="$(snapshot "$SO_R")"
refuses "run_archive_rejects_bad_tag"      run_archive "$SO_R" apply "../x" NAT
refuses "run_archive_rejects_bad_scenario" run_archive "$SO_R" apply tagR NAT unrelated_dir
refuses "run_archive_rejects_bad_mode"     run_archive "$SO_R" move tagR NAT
refuses "run_archive_rejects_outside_root" run_archive "$FIXTURE_ROOT/elsewhere" apply tagR NAT
check "run_archive_rejections_move_nothing" test "$BEFORE_R" = "$(snapshot "$SO_R")"

# Symlinked scenario dir is refused (only where the platform makes real symlinks).
ROOT_L="$(make_fixture)"
SO_L="$ROOT_L/outputs/simulations"
mkdir -p "$ROOT_L/elsewhere/SOC/2024/region_andes"
if ln -s "$ROOT_L/elsewhere/SOC" "$SO_L/SOC" 2>/dev/null && [ -L "$SO_L/SOC" ]; then
    export HPC_SCRATCH_ROOT="$ROOT_L"
    BEFORE_L="$(snapshot "$SO_L")"
    refuses "apply_symlinked_scenario_refuses" run_archive "$SO_L" apply tagL NAT SOC
    check "apply_symlinked_scenario_moves_nothing" test "$BEFORE_L" = "$(snapshot "$SO_L")"
else
    echo "SKIP: symlink case (platform cannot create real symlinks)"
fi

# ---------------------------------------------------------------------------
# Argument parsing as a subprocess (fails before any environment work)
# ---------------------------------------------------------------------------
arg_rc() {
    local rc=0
    env -u HPC_SCRATCH_ROOT bash "$SCRIPT" "$@" >/dev/null 2>&1 || rc=$?
    echo "$rc"
}
check "cli_unknown_flag_exit_2"     test "$(arg_rc --bogus)" -eq 2
check "cli_bad_tag_exit_2"          test "$(arg_rc --tag ../x)" -eq 2
check "cli_bad_scenario_exit_2"     test "$(arg_rc --scenarios 'NAT FOO')" -eq 2
check "cli_missing_tag_value_exit_2" test "$(arg_rc --tag)" -eq 2
check "cli_help_exit_0"             test "$(arg_rc --help)" -eq 0
check "cli_no_contract_exit_1"      test "$(arg_rc)" -eq 1

echo "============================="
echo "PASS: $PASS  FAIL: $FAIL"
[[ "$FAIL" -eq 0 ]]
