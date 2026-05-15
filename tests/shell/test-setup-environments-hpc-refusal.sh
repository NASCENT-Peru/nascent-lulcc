#!/usr/bin/env bash
# tests/shell/test-setup-environments-hpc-refusal.sh
#
# Phase 1.1 — D-112 / PIPE-04 — proves setup_environments.sh refuses to fall
# back to $PROJECT_ROOT/.envs on HPC when HPC_SCRATCH_ROOT is unset, AND
# confirms the workstation fallback path still works.
#
# Pure bash — no R / Dinamica / apptainer / micromamba required. Stubs
# micromamba via PATH prefix.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
SCRIPT="$REPO_ROOT/scripts/setup_environments.sh"
PASS=0
FAIL=0

make_micromamba_stub() {
    local d
    d="$(mktemp -d)"
    cat > "$d/micromamba" <<'STUB'
#!/usr/bin/env bash
# Test stub for micromamba — produces just enough output for setup_environments.sh
# to satisfy `find_micromamba` and the `shell hook -s bash` invocation. After
# the env-root line is printed, we want the script to exit BEFORE trying any
# real env creation. We achieve this by NOT providing the create subcommand
# and exiting non-zero on `env create`, which surfaces as a controlled error.
case "$1" in
    shell)
        shift
        if [[ "${1:-}" == "hook" ]]; then
            # Print a no-op activation shim
            echo 'micromamba() { :; }'
            exit 0
        fi
        ;;
    --version) echo "1.5.0"; exit 0 ;;
    env)
        shift
        case "${1:-}" in
            remove)
                exit 0
                ;;
            *) exit 0 ;;
        esac
        ;;
    create)
        # Non-zero so the script aborts AFTER printing the env-root line
        echo "TEST STUB: micromamba create not supported" >&2
        exit 2
        ;;
    *) exit 0 ;;
esac
STUB
    chmod +x "$d/micromamba"
    echo "$d"
}

run_case() {
    local name="$1"; shift
    local expected_exit="$1"; shift
    local must_match_stdout="$1"; shift
    local must_match_stderr="$1"; shift
    # Remaining args are the env-prefix: VAR1=val VAR2=val ... -- <script-args...>
    local env_args=()
    while [[ "${1:-}" != "--" ]]; do
        env_args+=("$1"); shift
    done
    shift  # drop --
    local script_args=("$@")

    local out err rc
    local out_file err_file
    out_file="$(mktemp)"; err_file="$(mktemp)"
    set +e
    ( env -i HOME="$HOME" USER="${USER:-testuser}" PATH="$STUB_DIR:/usr/bin:/bin" \
        MAMBA_EXE_CUSTOM="$STUB_DIR/micromamba" \
        "${env_args[@]}" bash "$SCRIPT" "${script_args[@]}" >"$out_file" 2>"$err_file" )
    rc=$?
    set -e
    out="$(cat "$out_file")"; err="$(cat "$err_file")"
    rm -f "$out_file" "$err_file"

    local ok=1
    if [[ -n "$expected_exit" && "$rc" -ne "$expected_exit" ]]; then ok=0; fi
    if [[ -n "$must_match_stdout" ]] && ! grep -qE "$must_match_stdout" <<<"$out"; then ok=0; fi
    if [[ -n "$must_match_stderr" ]] && ! grep -qE "$must_match_stderr" <<<"$err"; then ok=0; fi

    if [[ "$ok" -eq 1 ]]; then
        echo "PASS: $name"
        PASS=$((PASS+1))
    else
        echo "FAIL: $name"
        echo "  expected exit: $expected_exit  actual: $rc"
        echo "  stdout: $out"
        echo "  stderr: $err"
        FAIL=$((FAIL+1))
    fi
}

STUB_DIR="$(make_micromamba_stub)"
trap "rm -rf '$STUB_DIR'" EXIT

# Case 1: HPC refusal via SLURM_JOB_ID
run_case "hpc_refusal_via_slurm_env_var" 1 "" "HPC context detected.*SLURM env var.*HPC_SCRATCH_ROOT" \
    SLURM_JOB_ID=12345 -- --env allocation_env --non-interactive

# Case 2: HPC refusal via FORCE_HPC=true
run_case "hpc_refusal_via_force_hpc_env_var" 1 "" "HPC context detected.*--hpc flag.*HPC_SCRATCH_ROOT" \
    FORCE_HPC=true -- --env allocation_env --non-interactive

# Case 3: HPC refusal via --hpc CLI flag
run_case "hpc_refusal_via_hpc_cli_flag" 1 "" "HPC context detected.*--hpc flag.*HPC_SCRATCH_ROOT" \
    -- --env allocation_env --non-interactive --hpc

# Case 4: workstation fallback path (no signals, no HPC_SCRATCH_ROOT)
# The script will fail later when micromamba stub returns non-zero on env create,
# but the env-root line must be printed BEFORE that. Match the stdout signal.
run_case "workstation_fallback_prints_local_envs_path" "" "Env install root \\(local fallback" "" \
    -- --env allocation_env --non-interactive

# Case 5: HPC success path with HPC_SCRATCH_ROOT set
run_case "hpc_success_path_prints_scratch_envs" "" "Env install root \\(HPC\\): /tmp/test-scratch/micromamba/envs" "" \
    SLURM_JOB_ID=12345 HPC_SCRATCH_ROOT=/tmp/test-scratch -- --env allocation_env --non-interactive

echo "============================="
echo "PASS: $PASS  FAIL: $FAIL"
[[ "$FAIL" -eq 0 ]]
