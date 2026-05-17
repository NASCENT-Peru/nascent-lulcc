# Diagnostic Ladder — DinamicaConsole std::exception (Open Issue 1)

## Context

**Source of truth:** `.planning/phases/01.1-fix-dinamica-launch-contract/01.1-03-SUMMARY.md`
Open Issue 1, lines 104–138 — *"DinamicaConsole crashes with `std::exception` in the rebuilt `.sif`
regardless of `.ego` content"*.

**Refined hypothesis (post-ldd/LD_DEBUG diagnostic, 2026-05-17):**
The original library-compat hypothesis is **FALSIFIED**. Operator-side `ldd` inside the `.sif`
returned zero "not found" entries; glibc 2.34 needed vs 2.39 provided (fine); `LD_DEBUG=files`
showed all libraries map and `fini:` runs cleanly — meaning the C++ exception is thrown by
Dinamica's **own startup code** after library load.

Candidate causes (in order of prior probability):
1. The `cp -a squashfs-root/. . && rm -rf squashfs-root DinamicaEGO.AppImage` step in `%post`
   lines 87–89 dropped a non-squashfs side asset (e.g. a `Data/GDAL` or `Data/proj` subtree,
   or a Qt plugins dir).
2. A `DINAMICA_EGO_8_*` env var the wrapper sets that the `.sif` `%environment` block does NOT —
   `DinamicaConsole` bare invocation runs without the wrapper, so any wrapper-only env var would
   be unset.
3. A file that `DinamicaConsole` tries to read at startup (registry, license file, plugin manifest)
   that does not exist where expected.
4. A `.dinamica_ego_8.conf` key the staged-home copy is missing (the `.def` seeds only
   `AlternativePathForR`, `ClConfig`, `MemoryAllocationPolicy`, `RCranMirror`).

**Downstream:** Results feed Plan 01.1-06. The `diagnostics/FINDINGS.md` document (Task 3 below)
provides the concrete `.def` edit that Plan 06 implements. Without this ladder, Plan 06 would
be guesswork.

**Verification gaps this closes:**
- `.planning/phases/01.1-fix-dinamica-launch-contract/01.1-VERIFICATION.md` Gap 1 (SC2 — live
  smoke exits 5, not 0)
- Gap 2 (SC5 — `smoketest.ego` DinamicaConsole acceptance gated on Open Issue 1)

---

## Pre-requisites

Before running the ladder, verify all of the following on Euler:

1. **Host:** You are on an Euler login node (`eu-login-*`) or inside a `srun --pty bash`
   allocation with Internet access (Step 4 downloads a fresh AppImage).
2. **Apptainer:** `apptainer --version` shows ≥ 1.4.
3. **DINAMICA_EGO_8_HOME:** `echo $DINAMICA_EGO_8_HOME` resolves to the absolute path of the
   rebuilt `dinamica-ego-8.sif` (e.g. `/cluster/project/<project>/containers/dinamica-ego-8.sif`).
   The `.sif` must exist: `test -f "$DINAMICA_EGO_8_HOME" && echo OK`.
4. **HPC_SCRATCH_ROOT:** `echo $HPC_SCRATCH_ROOT` is set to your scratch root
   (e.g. `/cluster/scratch/$USER/nascent-lulcc`).
5. **Scratch subdirs:** Created: `mkdir -p "$HPC_SCRATCH_ROOT/dinamica-home" "$HPC_SCRATCH_ROOT/dinamica-tmp"`.
6. **REPO_ROOT:** `cd "$REPO_ROOT"` — your `nascent-lulcc` working tree on Euler
   (e.g. `export REPO_ROOT="$HPC_SCRATCH_ROOT/repo/nascent-lulcc"` or wherever you checked it out).
7. **Host tools:** `bash`, `find`, `diff`, `wc`, `curl` are on PATH on the login/compute node
   (host side). `strings` must be available inside the container (it typically is on Ubuntu Noble).
8. **strace:** `strace` availability is tested inside the container in Step 3. If it is absent,
   a fallback using the host `strace` against the container process is given.

---

## Diagnostic Ladder (4 steps)

Each step writes one or more evidence files under
`.planning/phases/01.1-fix-dinamica-launch-contract/diagnostics/`.
Copy-paste each command block verbatim; do **not** abbreviate or simplify the
`apptainer exec` invocations — the `--home` and `--env DINAMICA_EGO_8_TEMP_DIR` flags
must be present per D-105 (identical to the production launch shape).

---

### Step 1 — Capture the Data/ tree inside the `.sif`

**Purpose:** Confirms whether the GDAL data tree that the AppImage shipped is intact
or partially deleted by the `cp -a + rm -rf` step in `%post` lines 87–89.
**Hypothesis targeted:** Hypothesis 1 (dropped subtree).
**Evidence file:** `diagnostics/ls-data-tree.log`

```bash
set -euo pipefail
cd "$REPO_ROOT"
apptainer exec \
    --home "$HPC_SCRATCH_ROOT/dinamica-home" \
    --env "DINAMICA_EGO_8_TEMP_DIR=$HPC_SCRATCH_ROOT/dinamica-tmp" \
    "$DINAMICA_EGO_8_HOME" \
    bash -c 'ls -R /opt/dinamica/usr/bin/Data/ 2>&1; echo "---"; ls -la /opt/dinamica/usr/bin/ 2>&1' \
    > .planning/phases/01.1-fix-dinamica-launch-contract/diagnostics/ls-data-tree.log 2>&1
echo "Step 1 done: $(wc -l < .planning/phases/01.1-fix-dinamica-launch-contract/diagnostics/ls-data-tree.log) lines"
```

**Expected output:**
A non-empty log showing the `Data/` subtree (GDAL, proj, R, possibly Java/Qt subdirs).
If `Data/` is missing entirely OR truncated to just `R/`, this is Hypothesis 1 evidence.

---

### Step 2 — List every DINAMICA_EGO_8_* env var the binary references

**Purpose:** Identifies any env var `DinamicaConsole` references but that neither
the `.def` `%environment` block nor the `apptainer --env` override provides.
**Hypothesis targeted:** Hypothesis 2 (missing env var).
**Evidence file:** `diagnostics/strings-dinamica-env-vars.log`

```bash
apptainer exec \
    --home "$HPC_SCRATCH_ROOT/dinamica-home" \
    --env "DINAMICA_EGO_8_TEMP_DIR=$HPC_SCRATCH_ROOT/dinamica-tmp" \
    "$DINAMICA_EGO_8_HOME" \
    bash -c 'strings /opt/dinamica/usr/bin/DinamicaConsole | grep -E "DINAMICA_EGO_8_[A-Z_]+" | sort -u' \
    > .planning/phases/01.1-fix-dinamica-launch-contract/diagnostics/strings-dinamica-env-vars.log 2>&1
echo "Step 2 done: $(wc -l < .planning/phases/01.1-fix-dinamica-launch-contract/diagnostics/strings-dinamica-env-vars.log) lines"
```

**Expected output:**
A list of env var names like `DINAMICA_EGO_8_HOME`, `DINAMICA_EGO_8_INSTALLATION_DIRECTORY`,
`DINAMICA_EGO_8_TEMP_DIR`, `DINAMICA_EGO_8_GDAL_DATA`, `DINAMICA_EGO_8_LOG_PATH`, etc.
Cross-reference against the four vars `bin/DinamicaEGO.sh` sets (per CONTEXT.md D-104) and the
four `%environment` exports in the `.def`:

| Var | Source |
|-----|--------|
| `DINAMICA_EGO_8_INSTALLATION_DIRECTORY` | `%environment` (line 115) |
| `DINAMICA_EGO_8_HOME_DIR` | `%environment` (line 116) |
| `DINAMICA_EGO_8_TEMP_DIR` | `apptainer --env` at runtime (D-105) |
| `LD_LIBRARY_PATH` | `%environment` (line 113) |

Any var the binary references but no source covers is Hypothesis 2 evidence.

---

### Step 3 — strace the last 200 openat() syscalls before DinamicaConsole crashes

**Purpose:** Identifies the specific file `DinamicaConsole` failed to open immediately before
the `std::exception` crash — the smoking gun for Hypotheses 1 and 3.
**Hypothesis targeted:** Hypothesis 3 (specific startup file missing).
**Evidence file:** `diagnostics/strace-openat.log`

```bash
# First check whether strace is available inside the container
apptainer exec \
    --home "$HPC_SCRATCH_ROOT/dinamica-home" \
    --env "DINAMICA_EGO_8_TEMP_DIR=$HPC_SCRATCH_ROOT/dinamica-tmp" \
    "$DINAMICA_EGO_8_HOME" \
    bash -c 'which strace 2>&1 || echo "strace NOT found inside container"' \
    > /tmp/strace-availability.log 2>&1
cat /tmp/strace-availability.log

# If strace IS in the container:
if ! grep -q 'NOT found' /tmp/strace-availability.log; then
    apptainer exec \
        --home "$HPC_SCRATCH_ROOT/dinamica-home" \
        --env "DINAMICA_EGO_8_TEMP_DIR=$HPC_SCRATCH_ROOT/dinamica-tmp" \
        "$DINAMICA_EGO_8_HOME" \
        bash -c 'strace -f -e openat /opt/dinamica/usr/bin/DinamicaConsole 2>&1 | tail -200' \
        > .planning/phases/01.1-fix-dinamica-launch-contract/diagnostics/strace-openat.log 2>&1 || true
fi

# If strace is NOT in the container, use host strace to attach to the container process:
if [ ! -s .planning/phases/01.1-fix-dinamica-launch-contract/diagnostics/strace-openat.log ]; then
    echo "Attempting host-strace fallback..." >&2
    # Try to load strace from module system if not on PATH
    command -v strace >/dev/null 2>&1 || module load strace 2>/dev/null || true

    if command -v strace >/dev/null 2>&1; then
        apptainer exec \
            --home "$HPC_SCRATCH_ROOT/dinamica-home" \
            --env "DINAMICA_EGO_8_TEMP_DIR=$HPC_SCRATCH_ROOT/dinamica-tmp" \
            "$DINAMICA_EGO_8_HOME" \
            bash -c '/opt/dinamica/usr/bin/DinamicaConsole 2>&1' &
        DC_PID=$!
        strace -f -p $DC_PID -e openat 2>&1 | tail -200 \
            > .planning/phases/01.1-fix-dinamica-launch-contract/diagnostics/strace-openat.log 2>&1 || true
        wait $DC_PID 2>/dev/null || true
    else
        # Final fallback: ltrace with fopen/open equivalent
        apptainer exec \
            --home "$HPC_SCRATCH_ROOT/dinamica-home" \
            --env "DINAMICA_EGO_8_TEMP_DIR=$HPC_SCRATCH_ROOT/dinamica-tmp" \
            "$DINAMICA_EGO_8_HOME" \
            bash -c 'ltrace -e fopen+open /opt/dinamica/usr/bin/DinamicaConsole 2>&1 | tail -200 || echo "ltrace also unavailable"' \
            > .planning/phases/01.1-fix-dinamica-launch-contract/diagnostics/strace-openat.log 2>&1 || true
    fi
fi
echo "Step 3 done: $(wc -l < .planning/phases/01.1-fix-dinamica-launch-contract/diagnostics/strace-openat.log) lines"
```

**Expected output:**
The last few `openat()` syscalls before the binary aborts show the specific file
`DinamicaConsole` failed to open. Look for `ENOENT` (No such file or directory) return values
immediately before the process exits. If strace was not available at all, the log will contain
an error message — capture that verbatim and note it in the resume signal (Step 3 falls
back to "strace unavailable" rather than empty).

---

### Step 4 — Compare a fresh AppImage extract against the in-.sif tree

**Purpose:** Proves whether the `cp -a squashfs-root/. . && rm -rf squashfs-root DinamicaEGO.AppImage`
sequence in `%post` lines 87–89 dropped any non-squashfs side assets (e.g. a `Data/GDAL` dir
or `Qt` plugins directory).
**Hypothesis targeted:** Hypothesis 1 (dropped subtree — corroborating the ls output from Step 1).
**Evidence files:**
- `diagnostics/fresh-appimage-extract-tree.log`
- `diagnostics/sif-extract-tree.log`
- `diagnostics/extract-diff.log`

```bash
# Fresh extract of the AppImage in a temp dir on the host (Euler login node).
# Uses the same URL as dinamica/container/rocker-geospatial-dinamica.def line 69.
mkdir -p "$HPC_SCRATCH_ROOT/dinamica-appimage-fresh"
cd "$HPC_SCRATCH_ROOT/dinamica-appimage-fresh"
curl -fSL https://dinamicaego.com/nui_download/1960/ -o DinamicaEGO.AppImage
chmod +x DinamicaEGO.AppImage
./DinamicaEGO.AppImage --appimage-extract > /dev/null
find squashfs-root -maxdepth 4 -type d 2>&1 | sort \
    > "$REPO_ROOT/.planning/phases/01.1-fix-dinamica-launch-contract/diagnostics/fresh-appimage-extract-tree.log"
echo "Fresh extract tree: $(wc -l < "$REPO_ROOT/.planning/phases/01.1-fix-dinamica-launch-contract/diagnostics/fresh-appimage-extract-tree.log") dirs"

# In-.sif tree (post cp -a + rm -rf state)
apptainer exec \
    --home "$HPC_SCRATCH_ROOT/dinamica-home" \
    --env "DINAMICA_EGO_8_TEMP_DIR=$HPC_SCRATCH_ROOT/dinamica-tmp" \
    "$DINAMICA_EGO_8_HOME" \
    bash -c 'find /opt/dinamica/usr -maxdepth 4 -type d 2>&1 | sort' \
    > "$REPO_ROOT/.planning/phases/01.1-fix-dinamica-launch-contract/diagnostics/sif-extract-tree.log"
echo "SIF tree: $(wc -l < "$REPO_ROOT/.planning/phases/01.1-fix-dinamica-launch-contract/diagnostics/sif-extract-tree.log") dirs"

# Normalise prefixes and diff (apples-to-apples)
cd "$REPO_ROOT"
diff \
    <(sed -e 's|^squashfs-root/usr/|PREFIX/|' \
        .planning/phases/01.1-fix-dinamica-launch-contract/diagnostics/fresh-appimage-extract-tree.log) \
    <(sed -e 's|^/opt/dinamica/usr/|PREFIX/|' \
        .planning/phases/01.1-fix-dinamica-launch-contract/diagnostics/sif-extract-tree.log) \
    > .planning/phases/01.1-fix-dinamica-launch-contract/diagnostics/extract-diff.log || true
echo "Diff lines: $(wc -l < .planning/phases/01.1-fix-dinamica-launch-contract/diagnostics/extract-diff.log)"
echo "(0 diff lines = cp -a + rm -rf preserved everything; any < lines = dirs missing from .sif)"

# Clean up fresh extract to avoid filling scratch quota
rm -rf "$HPC_SCRATCH_ROOT/dinamica-appimage-fresh"
```

**Expected output:**
- `extract-diff.log` is **EMPTY** (0 lines) if `cp -a + rm -rf` preserved everything — Hypothesis 1
  is falsified.
- Any `<` lines in `extract-diff.log` = directories present in the fresh AppImage extract but
  missing from the `.sif` tree — Hypothesis 1 confirmed; those dirs are the probable fix target.

---

## Reporting Back

After completing all four steps:

### 1. Confirm evidence files are populated

```bash
wc -l "$REPO_ROOT/.planning/phases/01.1-fix-dinamica-launch-contract/diagnostics/"*.log
```

Typical line counts:
- `ls-data-tree.log`: 50–500 lines
- `strings-dinamica-env-vars.log`: 5–30 lines
- `strace-openat.log`: 50–200 lines (or "strace not available" if step 3 fell back)
- `fresh-appimage-extract-tree.log`: 50–200 lines
- `sif-extract-tree.log`: 30–200 lines
- `extract-diff.log`: 0 lines (if no drop) or up to 50 lines (if drop detected)

### 2. Stage and commit the evidence files

```bash
cd "$REPO_ROOT"
git add .planning/phases/01.1-fix-dinamica-launch-contract/diagnostics/*.log
git status .planning/phases/01.1-fix-dinamica-launch-contract/diagnostics/
# All six .log files should appear as new files; README.md already committed by Task 1.
git commit -m "diag(01.1-05): capture DinamicaConsole std::exception diagnostic ladder evidence

- ls-data-tree.log:               /opt/dinamica/usr/bin/Data/ subtree inside .sif
- strings-dinamica-env-vars.log:  DINAMICA_EGO_8_* vars referenced by binary
- strace-openat.log:              last openat() syscalls before std::exception
- fresh-appimage-extract-tree.log + sif-extract-tree.log + extract-diff.log:
  compares vanilla AppImage extract vs .def's cp -a + rm -rf result

Closes diagnostic ladder for 01.1-03 Open Issue 1; Plan 06 derives fix from this evidence.
Refs INFRA-01 SC2, MEM-06 SC5."
```

### 3. Resume signal

After committing, reply to the Claude Code session with one of:

- **`evidence-committed | diff: empty | strace-best-guess: <hypothesis 1/2/3/4>`** — all four
  steps ran cleanly; `extract-diff.log` is empty; strace points at hypothesis 1/2/3/4 (cite the
  suspect file/var/key).
- **`evidence-committed | diff: non-empty | drops: <names>`** — `extract-diff.log` is non-empty;
  the named subdirectories were dropped by `%post`'s `cp -a + rm -rf` and are the most likely
  cause.
- **`partial: <which-step-failed>`** — at least one step could not run (e.g. "strace not
  available, ltrace fallback used"); evidence is partial; describe what was captured.
- **`blocked: <reason>`** — cannot run on Euler right now; propose retry date.

If any step is unsupported (e.g. `strace` genuinely unavailable both inside the `.sif` and on
the host), capture the error verbatim in the `.log` file and note it in the resume signal. Task 3
(autonomous synthesis) will treat that hypothesis as "unverified" rather than "falsified".

---

## Evidence File Index

| File | Produced by | Hypothesis | Expected size |
|------|------------|-----------|--------------|
| `ls-data-tree.log` | Step 1 | H1 (dropped Data/ subtree) | 50–500 lines |
| `strings-dinamica-env-vars.log` | Step 2 | H2 (missing env var) | 5–30 lines |
| `strace-openat.log` | Step 3 | H3 (startup file missing) | 50–200 lines |
| `fresh-appimage-extract-tree.log` | Step 4 | H1 corroboration | 50–200 lines |
| `sif-extract-tree.log` | Step 4 | H1 corroboration | 30–200 lines |
| `extract-diff.log` | Step 4 | H1 proof (empty = falsified) | 0–50 lines |

After Task 3 synthesises these into `FINDINGS.md`, Plan 01.1-06 implements the top-ranked fix.

---

*Phase: 01.1-fix-dinamica-launch-contract*
*Plan: 05 (gap closure)*
*Created by: Task 1 of Plan 01.1-05*
*Source: 01.1-03-SUMMARY.md Open Issue 1 (lines 104–138)*
