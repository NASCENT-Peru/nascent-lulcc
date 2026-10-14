#!/bin/bash
# archive_allocation_outputs.sh
# Move existing allocation output trees out of the way before a fresh sweep.
#
# WHERE TO RUN: on login02, from the code tree /home/black/nascent-lulcc, after
# `source .env`. This is NOT an sbatch job — it only lists files and renames
# directories, and it is safe to run on the login node.
#
#   cd /home/black/nascent-lulcc
#   source .env
#   bash scripts/archive_allocation_outputs.sh                      # dry run (default)
#   bash scripts/archive_allocation_outputs.sh --tag pre_interventions_phase5
#   bash scripts/archive_allocation_outputs.sh --tag pre_interventions_phase5 --apply
#
# WHY THIS EXISTS: scripts/submit_allocation_scenario.sh skips a region whose
# posteriors all exist ($SIM_OUT/$SCENARIO/$YEAR/region_<suffix>/posterior.tif,
# the file-presence check at its lines 174-180), and the allocation driver
# resumes a region at the first missing posterior, chaining off the previous
# year's posterior.tif as the next anterior. Posteriors produced by older code
# (for example before spatial interventions were wired in) would therefore be
# treated as finished work or used as chain inputs. Moving each
# $SIM_OUT/<scenario> tree to $SIM_OUT/_archive_<tag>/<scenario> takes them out
# of that path pattern so the sweep starts from the initial LULC again.
#
# WHAT IT DOES: for each of BAU NAT CUL SOC (or --scenarios), counts region
# posteriors and national mosaics with `find` (file names only), prints the
# exact source -> destination, and with --apply renames the tree with `mv -T`
# on the same filesystem. A manifest of every move (ARCHIVE_MANIFEST.tsv) is
# written inside the archive dir. A renamed tree can be put back with
# `mv -T $SIM_OUT/_archive_<tag>/<scenario> $SIM_OUT/<scenario>`.
#
# WHAT IT NEVER DOES: no terra, no raster reads, no file content reads, no
# copying and no deleting — rename only. It refuses to move across
# filesystems (which `mv` would silently turn into copy + delete).
#
# The only R process is the get_config() probe for simulation_output_dir. It
# is a deliberate copy of the probe in submit_allocation_scenario.sh, which the
# sweep launcher already runs on login02 every time: it loads only the yaml
# namespace (src/setup.r), and like the launcher it creates any configured
# input/output directory that is missing.
#
# Refusals (exit 1, nothing moved): Stage 7 contract incomplete;
# HPC_SCRATCH_ROOT not ending in /nascent-lulcc (a stale root such as
# /beegfs/black passes the contract but resolves the wrong tree); SIM_OUT empty,
# "/", relative, not a directory or not under $HPC_SCRATCH_ROOT/; a scenario dir
# that is a symlink or not a directory; an archive target that already exists;
# source and archive on different filesystems; and, for --apply only, any
# allocation, mosaic or verifier job of $USER still queued or running.
#
# Exit status: 0 success, 1 refused or failed, 2 usage error.

ARCHIVE_SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

ARCHIVE_ALLOWED_SCENARIOS=(BAU NAT CUL SOC)
ARCHIVE_DEFAULT_SCENARIOS="BAU NAT CUL SOC"

# SLURM job names whose jobs read or write $SIM_OUT/<scenario>. A tree must
# not be renamed underneath any of them.
ARCHIVE_GUARDED_JOB_NAMES=(
    alloc-region
    lulc-allocation-smoke
    assemble-mosaic
    iv-verify
    wr04-probe
    lulc-allocation
    lulc-allocation-profile
    lulc-dinamica-only
)

archive_usage() {
    cat <<'EOF'
Usage: bash scripts/archive_allocation_outputs.sh [--apply] [--tag TAG] [--scenarios "NAT CUL"]

Run on login02 from the code tree after `source .env`. Not an sbatch job.

Renames each existing $SIM_OUT/<scenario> tree to $SIM_OUT/_archive_<TAG>/<scenario>
so the sweep launcher and the allocation driver no longer find its posteriors.

  (no flags)     Dry run: print counts and the planned moves, change nothing.
  --apply        Perform the renames (refused while allocation jobs are queued
                 or running for $USER).
  --tag TAG      Archive dir suffix; letters, digits, '.', '_', '-' only, no '..'.
                 Default: pre_interventions_YYYYMMDD (today's date).
  --scenarios S  Space-separated subset of BAU NAT CUL SOC (default: all four).
  -h, --help     Show this help.

Environment:
  ARCHIVE_ALLOW_ANY_SCRATCH_ROOT=1  Accept an HPC_SCRATCH_ROOT that does not end
                                    in /nascent-lulcc. Leave unset on the cluster:
                                    a stale root such as /beegfs/black resolves
                                    the wrong output tree.

SIM_OUT is resolved from get_config() (simulation_output_dir), exactly as
scripts/submit_allocation_scenario.sh does. Nothing is copied or deleted.
EOF
}

# ----------------------------------------------------------
# Validators
# ----------------------------------------------------------

# validate_sim_out <path>: the resolved output dir must be an existing absolute
# directory strictly below $HPC_SCRATCH_ROOT/.
validate_sim_out() {
    local p="${1:-}"
    local root="${HPC_SCRATCH_ROOT:-}"

    if [ -z "$root" ] || [ "$root" = "/" ]; then
        echo "ERROR: HPC_SCRATCH_ROOT is empty or '/'; refusing to resolve any output path from it." >&2
        return 1
    fi
    root="${root%/}"
    case "$root" in
        /*) ;;
        *)
            echo "ERROR: HPC_SCRATCH_ROOT is not an absolute path: $root" >&2
            return 1
            ;;
    esac

    if [ -z "$p" ]; then
        echo "ERROR: simulation output dir is empty (get_config() returned nothing)." >&2
        return 1
    fi
    if [ "$p" = "/" ]; then
        echo "ERROR: simulation output dir is '/'; refusing." >&2
        return 1
    fi
    case "$p" in
        /*) ;;
        *)
            echo "ERROR: simulation output dir is not an absolute path: $p" >&2
            return 1
            ;;
    esac
    p="${p%/}"
    case "$p/" in
        */../*|*/./*)
            echo "ERROR: simulation output dir contains '.' or '..' components: $p" >&2
            return 1
            ;;
    esac
    if [ ! -d "$p" ]; then
        echo "ERROR: simulation output dir does not exist or is not a directory: $p" >&2
        return 1
    fi
    case "$p" in
        "$root"/*) ;;
        *)
            echo "ERROR: simulation output dir is not under HPC_SCRATCH_ROOT ($root/): $p" >&2
            return 1
            ;;
    esac
    return 0
}

# validate_tag <tag>: the tag becomes a path component, so only a safe charset.
validate_tag() {
    local tag="${1:-}"
    if [ -z "$tag" ]; then
        echo "ERROR: --tag is empty." >&2
        return 1
    fi
    if ! [[ "$tag" =~ ^[A-Za-z0-9._-]+$ ]]; then
        echo "ERROR: --tag '$tag' may contain only letters, digits, '.', '_' and '-'." >&2
        return 1
    fi
    case "$tag" in
        *..*)
            echo "ERROR: --tag '$tag' must not contain '..'." >&2
            return 1
            ;;
    esac
    return 0
}

# validate_scenarios <s...>: each must be one of BAU NAT CUL SOC, no repeats.
validate_scenarios() {
    if [ "$#" -eq 0 ]; then
        echo "ERROR: no scenarios given." >&2
        return 1
    fi
    local s allowed ok seen=" "
    for s in "$@"; do
        ok=0
        for allowed in "${ARCHIVE_ALLOWED_SCENARIOS[@]}"; do
            if [ "$s" = "$allowed" ]; then ok=1; break; fi
        done
        if [ "$ok" -ne 1 ]; then
            echo "ERROR: unknown scenario '$s' (allowed: ${ARCHIVE_ALLOWED_SCENARIOS[*]})." >&2
            return 1
        fi
        case "$seen" in
            *" $s "*)
                echo "ERROR: scenario '$s' listed more than once." >&2
                return 1
                ;;
        esac
        seen="$seen$s "
    done
    return 0
}

# ----------------------------------------------------------
# SIM_OUT resolution (copy of the sweep launcher's probe)
# ----------------------------------------------------------

# resolve_sim_out: sets the global SIM_OUT. Deliberate copy of the probe in
# scripts/submit_allocation_scenario.sh (env setup, activation, Rscript check,
# then get_config()[['simulation_output_dir']], first output line, trimmed) so
# this script archives exactly the tree the launcher will look in. The launcher
# itself is left untouched. Requires hpc_common.sh to be sourced and
# PROJECT_ROOT to point at the code tree.
resolve_sim_out() {
    SIM_OUT=""
    export PROJECT_ROOT
    local env_path="$ENV_BASE_PATH/allocation_env"
    local rscript_bin
    local -a probe_out=()

    # Fail closed on env setup/activation (same reasoning as the launcher's
    # WR-04 guard: an unchecked failure would let a stale on-PATH Rscript
    # resolve the config). Their progress messages go to stderr so stdout
    # carries only this script's report.
    setup_common_env >&2 || { echo "ERROR: setup_common_env failed" >&2; return 1; }
    activate_env "$env_path" >&2 || { echo "ERROR: activate_env failed for $env_path" >&2; return 1; }
    rscript_bin=$(verify_rscript "$env_path")
    if [ $? -ne 0 ] || [ -z "$rscript_bin" ]; then
        echo "ERROR: no Rscript in $env_path" >&2
        return 1
    fi

    readarray -t probe_out < <("$rscript_bin" --vanilla -e "setwd(Sys.getenv('PROJECT_ROOT')); source('src/setup.r'); cfg <- get_config(); cat(cfg[['simulation_output_dir']], '\n')" 2>/dev/null)
    SIM_OUT="${probe_out[0]:-}"
    SIM_OUT="$(echo "$SIM_OUT" | xargs)"   # trim
    if [ -z "$SIM_OUT" ]; then
        echo "ERROR: could not resolve simulation_output_dir from get_config()." >&2
        return 1
    fi
    return 0
}

# ----------------------------------------------------------
# Running-job guard
# ----------------------------------------------------------

# running_allocation_jobs: print one "<jobid> <name> <state>" line per queued or
# running job of $USER whose name is in ARCHIVE_GUARDED_JOB_NAMES. If squeue
# exists but fails, print a sentinel line so --apply refuses (an unknown queue
# state is not an empty queue). If squeue is absent, print nothing and warn.
running_allocation_jobs() {
    if ! command -v squeue >/dev/null 2>&1; then
        echo "WARNING: squeue not found; cannot check for running allocation jobs." >&2
        return 0
    fi
    local user="${USER:-$(id -un 2>/dev/null)}"
    local listing rc=0
    listing="$(squeue -h -u "$user" -o '%i %j %T' 2>/dev/null)" || rc=$?
    if [ "$rc" -ne 0 ]; then
        echo "UNKNOWN squeue-failed rc=$rc"
        return 0
    fi
    [ -n "$listing" ] || return 0
    printf '%s\n' "$listing" | awk -v names="${ARCHIVE_GUARDED_JOB_NAMES[*]}" '
        BEGIN { n = split(names, a, " "); for (i = 1; i <= n; i++) want[a[i]] = 1 }
        ($2 in want) { print }
    '
}

# ----------------------------------------------------------
# Counting (file names only, never contents)
# ----------------------------------------------------------

# list_region_posteriors <scenario_dir>: <dir>/<year>/region_<suffix>/posterior.tif
list_region_posteriors() {
    find "$1" -mindepth 3 -maxdepth 3 -path '*/region_*/posterior.tif' 2>/dev/null | LC_ALL=C sort
}

# list_national_mosaics <scenario_dir>: <dir>/<year>/posterior_<year>.tif
list_national_mosaics() {
    find "$1" -mindepth 2 -maxdepth 2 -name 'posterior_*.tif' 2>/dev/null | LC_ALL=C sort
}

count_lines() {
    if [ -z "$1" ]; then echo 0; else printf '%s\n' "$1" | wc -l | tr -d ' '; fi
}

# report_scenario <sim_out> <scenario> <archive_root> [verb]: print what is in
# the scenario tree and where it would go. Returns 0 for a present directory
# or an absent one, 1 for anything that must not be moved.
report_scenario() {
    local sim_out="${1%/}" scenario="$2" archive_root="$3" verb="${4:-would move}"
    local d="$sim_out/$scenario"
    local dst="$archive_root/$scenario"

    if [ -L "$d" ]; then
        echo "[$scenario] ERROR: $d is a symlink; refusing to move it (rename the link target by hand)." >&2
        return 1
    fi
    if [ ! -e "$d" ]; then
        echo "[$scenario] absent - nothing to move"
        return 0
    fi
    if [ ! -d "$d" ]; then
        echo "[$scenario] ERROR: $d exists but is not a directory; refusing." >&2
        return 1
    fi

    local posts mosaics years
    posts="$(list_region_posteriors "$d")"
    mosaics="$(list_national_mosaics "$d")"
    echo "[$scenario] region posteriors: $(count_lines "$posts")"
    echo "[$scenario] national mosaics: $(count_lines "$mosaics")"
    if [ -n "$posts" ]; then
        years="$(printf '%s\n' "$posts" | awk -F/ '{print $(NF-2)}' | LC_ALL=C sort -u | tr '\n' ' ')"
        echo "[$scenario] posterior years: ${years% }"
        echo "[$scenario] per region:"
        printf '%s\n' "$posts" | awk -F/ '{print $(NF-1)}' | LC_ALL=C sort | uniq -c
    else
        echo "[$scenario] posterior years: none"
    fi
    if [ -f "$d/run_manifest.csv" ]; then
        echo "[$scenario] run_manifest.csv: present"
    fi
    echo "[$scenario] $verb: $d -> $dst"
    return 0
}

# ----------------------------------------------------------
# Archive
# ----------------------------------------------------------

archive_code_head() {
    local root="${PROJECT_ROOT:-$ARCHIVE_SCRIPT_DIR/..}"
    git -C "$root" rev-parse --short HEAD 2>/dev/null || echo unknown
}

# append_manifest_row <manifest> <scenario> <src> <dst> <posteriors> <mosaics>
append_manifest_row() {
    local manifest="$1"
    if [ ! -e "$manifest" ]; then
        printf 'timestamp\tuser\thost\tcode_head\tscenario\tsource\tdestination\tregion_posteriors\tnational_mosaics\n' \
            > "$manifest" || return 1
    fi
    printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
        "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
        "${USER:-$(id -un 2>/dev/null)}" \
        "$(hostname 2>/dev/null || uname -n)" \
        "$(archive_code_head)" \
        "$2" "$3" "$4" "$5" "$6" >> "$manifest"
}

# run_archive <sim_out> <dry-run|apply> <tag> <scenario...>
run_archive() {
    if [ "$#" -lt 4 ]; then
        echo "ERROR: run_archive needs <sim_out> <mode> <tag> <scenario...>." >&2
        return 2
    fi
    local sim_out="${1%/}" mode="$2" tag="$3"
    shift 3
    local -a scenarios=("$@")

    case "$mode" in
        dry-run|apply) ;;
        *)
            echo "ERROR: unknown mode '$mode' (expected dry-run or apply)." >&2
            return 2
            ;;
    esac
    validate_sim_out "$sim_out" || return 1
    validate_tag "$tag" || return 1
    validate_scenarios "${scenarios[@]}" || return 1

    local archive_root="$sim_out/_archive_$tag"
    local manifest="$archive_root/ARCHIVE_MANIFEST.tsv"

    echo "========================================="
    echo "Archive allocation outputs"
    echo "========================================="
    echo "SIM_OUT:   $sim_out"
    echo "Archive:   $archive_root"
    echo "Mode:      $mode"
    echo "Scenarios: ${scenarios[*]}"
    echo

    # Top-level entries this run does not touch.
    local name s keep
    while IFS= read -r name; do
        [ -n "$name" ] || continue
        keep=1
        for s in "${scenarios[@]}"; do
            if [ "$name" = "$s" ]; then keep=0; break; fi
        done
        if [ "$keep" -eq 1 ]; then
            echo "left in place: $name"
        fi
    done < <(find "$sim_out" -mindepth 1 -maxdepth 1 -printf '%f\n' 2>/dev/null | LC_ALL=C sort)
    echo

    # Report every scenario; collect the present ones and their counts.
    local verb="would move"
    [ "$mode" = "apply" ] && verb="will move"
    local -a present=()
    local -A n_post=() n_mosaic=()
    local bad=0 d
    for s in "${scenarios[@]}"; do
        if ! report_scenario "$sim_out" "$s" "$archive_root" "$verb"; then
            bad=$((bad+1))
            continue
        fi
        d="$sim_out/$s"
        if [ -d "$d" ] && [ ! -L "$d" ]; then
            present+=("$s")
            n_post[$s]="$(count_lines "$(list_region_posteriors "$d")")"
            n_mosaic[$s]="$(count_lines "$(list_national_mosaics "$d")")"
        fi
        echo
    done
    if [ "$bad" -gt 0 ]; then
        echo "ERROR: $bad scenario dir(s) cannot be archived safely; nothing moved." >&2
        return 1
    fi

    # Running-job check.
    local jobs
    jobs="$(running_allocation_jobs)"
    if [ -n "$jobs" ]; then
        if [ "$mode" = "apply" ]; then
            echo "ERROR: allocation/mosaic/verifier jobs are queued or running; refusing to move anything:" >&2
            printf '  %s\n' "$jobs" >&2
            echo "Wait for them to finish (squeue -u ${USER:-$(id -un 2>/dev/null)}) and re-run." >&2
            return 1
        fi
        echo "WARNING: these jobs are queued or running; --apply will refuse until they finish:"
        printf '  %s\n' "$jobs"
        echo
    fi

    if [ "$mode" = "dry-run" ]; then
        echo "Dry run: nothing moved. Re-run with --apply to perform the moves above."
        echo "STATE archive_allocation_outputs mode=$mode sim_out=$sim_out archive=$archive_root moved=0"
        return 0
    fi

    if [ "${#present[@]}" -eq 0 ]; then
        echo "Nothing to move."
        echo "STATE archive_allocation_outputs mode=$mode sim_out=$sim_out archive=$archive_root moved=0"
        return 0
    fi

    # Pre-checks before ANY move: archive root sane, no target exists, every
    # source on the same filesystem as SIM_OUT (so mv is a rename).
    if [ -L "$archive_root" ] || { [ -e "$archive_root" ] && [ ! -d "$archive_root" ]; }; then
        echo "ERROR: $archive_root exists and is not a plain directory; nothing moved." >&2
        return 1
    fi
    local conflicts=0 dst
    for s in "${present[@]}"; do
        dst="$archive_root/$s"
        if [ -e "$dst" ] || [ -L "$dst" ]; then
            echo "ERROR: archive target already exists: $dst" >&2
            conflicts=$((conflicts+1))
        fi
    done
    if [ "$conflicts" -gt 0 ]; then
        echo "ERROR: $conflicts archive target(s) already exist; choose another --tag. Nothing moved." >&2
        return 1
    fi
    local dev_root dev_src
    dev_root="$(stat -c %d "$sim_out" 2>/dev/null)"
    for s in "${present[@]}"; do
        dev_src="$(stat -c %d "$sim_out/$s" 2>/dev/null)"
        if [ -z "$dev_root" ] || [ -z "$dev_src" ] || [ "$dev_src" != "$dev_root" ]; then
            echo "ERROR: $sim_out/$s is not on the same filesystem as $sim_out (device '$dev_src' vs '$dev_root'); a move would copy. Nothing moved." >&2
            return 1
        fi
    done

    mkdir -p "$archive_root" || { echo "ERROR: cannot create $archive_root; nothing moved." >&2; return 1; }
    local dev_arch
    dev_arch="$(stat -c %d "$archive_root" 2>/dev/null)"

    local moved=0 src
    local -a done_list=()
    for s in "${present[@]}"; do
        src="$sim_out/$s"
        dst="$archive_root/$s"
        dev_src="$(stat -c %d "$src" 2>/dev/null)"
        if [ -z "$dev_arch" ] || [ "$dev_src" != "$dev_arch" ]; then
            echo "ERROR: $src and $archive_root are on different filesystems; stopping before $s." >&2
            echo "  moved so far: ${done_list[*]:-none}" >&2
            echo "  not moved:    ${present[*]:$moved}" >&2
            echo "STATE archive_allocation_outputs mode=$mode sim_out=$sim_out archive=$archive_root moved=$moved"
            return 1
        fi
        if ! mv -T -- "$src" "$dst"; then
            echo "ERROR: mv failed for $src -> $dst; stopping." >&2
            echo "  moved so far: ${done_list[*]:-none}" >&2
            echo "  not moved:    ${present[*]:$moved}" >&2
            echo "STATE archive_allocation_outputs mode=$mode sim_out=$sim_out archive=$archive_root moved=$moved"
            return 1
        fi
        moved=$((moved+1))
        done_list+=("$s")
        echo "[$s] moved: $src -> $dst"
        if ! append_manifest_row "$manifest" "$s" "$src" "$dst" "${n_post[$s]}" "${n_mosaic[$s]}"; then
            echo "WARNING: could not write manifest row for $s to $manifest (the move itself succeeded)." >&2
        fi
    done

    # Post-check: the launcher pattern must find no posterior for these scenarios.
    local left=0 f
    for s in "${scenarios[@]}"; do
        for f in "$sim_out/$s"/*/region_*/posterior.tif; do
            [ -e "$f" ] && left=$((left+1))
        done
    done
    echo
    echo "Manifest: $manifest"
    echo "Region posteriors still under \$SIM_OUT/{${scenarios[*]}}: $left"
    echo "STATE archive_allocation_outputs mode=$mode sim_out=$sim_out archive=$archive_root moved=$moved"
    if [ "$left" -ne 0 ]; then
        echo "ERROR: $left region posterior(s) still match the launcher pattern after the moves." >&2
        return 1
    fi
    return 0
}

# ----------------------------------------------------------
# Entry point
# ----------------------------------------------------------

main() {
    local mode="dry-run" tag="" scen_str="$ARCHIVE_DEFAULT_SCENARIOS"
    while [ "$#" -gt 0 ]; do
        case "$1" in
            --apply) mode="apply" ;;
            --tag)
                if [ "$#" -lt 2 ]; then
                    echo "ERROR: --tag needs a value." >&2
                    archive_usage >&2
                    return 2
                fi
                tag="$2"
                shift
                ;;
            --tag=*) tag="${1#--tag=}" ;;
            --scenarios)
                if [ "$#" -lt 2 ]; then
                    echo "ERROR: --scenarios needs a value." >&2
                    archive_usage >&2
                    return 2
                fi
                scen_str="$2"
                shift
                ;;
            --scenarios=*) scen_str="${1#--scenarios=}" ;;
            -h|--help)
                archive_usage
                return 0
                ;;
            *)
                echo "ERROR: unknown argument: $1" >&2
                archive_usage >&2
                return 2
                ;;
        esac
        shift
    done

    [ -n "$tag" ] || tag="pre_interventions_$(date +%Y%m%d)"
    validate_tag "$tag" || return 2
    local -a scenarios=()
    read -r -a scenarios <<< "$scen_str"
    validate_scenarios ${scenarios[@]+"${scenarios[@]}"} || return 2

    PROJECT_ROOT="$(cd "$ARCHIVE_SCRIPT_DIR/.." && pwd)"
    export PROJECT_ROOT
    cd "$PROJECT_ROOT" || { echo "ERROR: cannot cd to PROJECT_ROOT: $PROJECT_ROOT" >&2; return 1; }

    # shellcheck source=hpc_common.sh
    source "$ARCHIVE_SCRIPT_DIR/hpc_common.sh"

    if ! check_stage7_contract; then
        echo "Run 'source .env' in the code tree first. Nothing moved." >&2
        return 1
    fi
    case "${HPC_SCRATCH_ROOT%/}" in
        */nascent-lulcc) ;;
        *)
            if [ "${ARCHIVE_ALLOW_ANY_SCRATCH_ROOT:-0}" = "1" ]; then
                echo "WARNING: HPC_SCRATCH_ROOT=$HPC_SCRATCH_ROOT does not end in /nascent-lulcc; continuing because ARCHIVE_ALLOW_ANY_SCRATCH_ROOT=1." >&2
            else
                echo "ERROR: HPC_SCRATCH_ROOT=$HPC_SCRATCH_ROOT does not end in /nascent-lulcc." >&2
                echo "       A stale root such as /beegfs/black passes the contract check but resolves" >&2
                echo "       the wrong output tree. Fix .env and 'source .env' again." >&2
                echo "       (Override only if intended: ARCHIVE_ALLOW_ANY_SCRATCH_ROOT=1.) Nothing moved." >&2
                return 1
            fi
            ;;
    esac

    resolve_sim_out || return 1
    validate_sim_out "$SIM_OUT" || return 1
    run_archive "$SIM_OUT" "$mode" "$tag" "${scenarios[@]}"
}

# Run main only when executed, not when sourced (the test sources this file).
if [ "${BASH_SOURCE[0]}" = "$0" ]; then
    set -uo pipefail
    main "$@"
    exit $?
fi
