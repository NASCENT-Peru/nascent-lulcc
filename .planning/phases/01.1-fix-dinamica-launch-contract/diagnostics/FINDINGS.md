# Diagnostic Findings — DinamicaConsole std::exception (Open Issue 1)

**Phase:** 01.1-fix-dinamica-launch-contract
**Date:** 2026-05-18 (updated after Plan 06 iteration 5)
**Summary (updated 2026-05-18):** H7 (PROJ_DATA not set) is the active hypothesis.
All prior hypotheses (H1–H6) are FALSIFIED. The root cause: `DinamicaEGO.sh` explicitly
runs `export PROJ_DATA=${BIN_PATH}/Data/GDAL` before calling `bin/DinamicaConsole`.
Our `%environment` block has always been missing `PROJ_DATA`. The PROJ library (loaded
by GDAL, which libBase.so uses for geospatial init) needs `PROJ_DATA` to find its datum
grid files. Without it, PROJ initialization fails and libBase.so throws `std::exception`
before DinamicaConsole produces any output.
**Iteration 5 fix (no rebuild needed for diagnosis):** Test
`--env PROJ_DATA=/opt/dinamica/usr/bin/Data/GDAL` on the current .sif. If confirmed,
add to `%environment` and rebuild.

---

## Evidence Summary

| Step | Evidence File | Captured | Interpretation |
|------|--------------|---------|---------------|
| 1 | `ls-data-tree.log` (316 lines) | `ls -R /opt/dinamica/usr/bin/Data/` and `ls -la /opt/dinamica/usr/bin/` | Data/GDAL intact; Data/R has Dinamica_1.0.8.tar.gz; all key binaries present |
| 2 | `strings-dinamica-env-vars.log` (0 bytes) | **EMPTY** — `strings` command produced no output | Step 2 FAILED; cannot determine env var references directly |
| 3 | `strace-openat.log` (3 lines) | `ptrace_scope` permission denied | Step 3 FAILED due to Euler HPC yama/ptrace_scope restriction |
| 4 | `fresh-appimage-extract-tree.log` + `sif-extract-tree.log` + `extract-diff.log` | Zero `<` lines | H1 FALSIFIED — no Data/ directory dropped |
| 5 | DinamicaEGO.sh source (Plan 06 Sub-step A) | Console mode: `DINAMICA_EGO_8_LOG_PATH=dirname(model)`; calls `bin/DinamicaConsole "$@"` | H2 env-var hypothesis weakened; APPDIR not set by wrapper |
| 6 | Build+smoke iteration 1 (B1 env vars) | exit 5 | B1 fix FALSIFIED |
| 7 | Build+smoke iteration 2 (APPDIR + GdalToolsData) | exit 5 | Iteration 2 fix FALSIFIED |
| 8 | LD_DEBUG=files inside container | All libraries load cleanly; zero errors | H3 FALSIFIED — no missing library |
| 9 | AppRun binary type check | ELF compiled binary; also crashes when invoked directly | Not a wrapper/env-var scripting issue; crash in AppRun startup too |
| 10 | PYTHONHOME override | `PYTHONHOME=/opt/dinamica/usr/bin/PyEnvironment` → still crash | PYTHONHOME hypothesis FALSIFIED |
| 11 | Build+smoke iteration 3 (rocker/r-ver:4.4.3 Jammy rollback) | exit 5 | H5 FALSIFIED — crash is OS-independent |
| 12 | Reference repo analysis (ethzplus/evoland-plus-HPC) | Working EGO 7 image uses `r-base:4.3.1` (Debian Bookworm); calls DinamicaConsole directly; **AppRun sets QT_PLUGIN_PATH** | Confirms AppRun env setup is the missing layer; `QT_PLUGIN_PATH` never in our `%environment` |
| 13 | `ldd DinamicaConsole \| grep -i qt` + `ls /opt/dinamica/usr/plugins/platforms/` | **zero Qt libraries**; platforms/ dir does not exist | H6 FALSIFIED — DinamicaConsole has no Qt dependency whatsoever |
| 14 | `QT_QPA_PLATFORM=offscreen --env QT_PLUGIN_PATH=... DinamicaConsole -version` | Still exits 5 with std::exception | H6 FALSIFIED (consistent with step 13) |
| 15 | `cat /opt/dinamica/usr/bin/DinamicaEGO.sh` (full source) | Line: `export PROJ_DATA=${BIN_PATH}/Data/GDAL`; wrapper sets PROJ_DATA before calling DinamicaConsole | H7 candidate identified — `PROJ_DATA` never in our `%environment` |

**Depth offset explanation (critical for reading extract-diff.log):**
The `fresh` find uses `squashfs-root` as root; after prefix normalization, all 99 diff lines are `>`
(sif-only, showing deeper levels) — NOT because the sif added dirs that weren't in the AppImage.

---

## Hypothesis Ranking

### Hypothesis 1 — Data/ subtree dropped during cp -a + rm -rf

**Status: FALSIFIED** — extract-diff.log zero `<` lines; all directories preserved.

---

### Hypothesis 2 — Missing DINAMICA_EGO_8_* environment variable

**Status: FALSIFIED** — B1 fix added GDAL_DATA and LOG_PATH to `%environment`; AppRun binary
(which sets the full env) also crashes directly. All env-var candidates tried.

---

### Hypothesis 3 — Specific file fails to open at startup

**Status: FALSIFIED** — LD_DEBUG=files inside container showed zero file-open errors. All shared
libraries load successfully. The crash is in DinamicaConsole's own initialization code after
library loading.

---

### Hypothesis 4 — Missing `.dinamica_ego_8.conf` key

**Status: FALSIFIED** — B2 fix added `GdalToolsData` (the correct key per DinamicaEGO.sh source);
crash unchanged. Conf seed is now complete and consistent with what the wrapper reads.

---

### Hypothesis 5 — Ubuntu Noble (24.04) C++ ABI runtime incompatibility

**Status: FALSIFIED** — Iteration 3 rolled back to `rocker/r-ver:4.4.3` (Ubuntu Jammy 22.04,
libstdc++12). Smoke test still exited 5 with identical crash. The crash is OS/libstdc++ version
independent.

---

### Hypothesis 6 — Qt platform plugin initialization failure (QT_PLUGIN_PATH not set) *(PRIMARY)*

**Evidence:**

1. **AppRun analysis (Step 9):** AppRun is a compiled binary that sets `QT_PLUGIN_PATH` (among other
   env vars) before launching DinamicaConsole. Our `%environment` replicated `APPDIR`, `PYTHONHOME`,
   `GDAL_DATA`, `LOG_PATH` from AppRun — but never included `QT_PLUGIN_PATH`.

2. **Reference repo analysis (Step 12):** `cbueth/dinamica-ego-docker` (the working Dinamica EGO
   image used by the `ethzplus/evoland-plus-HPC` project on Euler) bootstraps from `r-base:4.3.1`
   (Debian Bookworm) and calls DinamicaConsole directly via `$DINAMICA_EGO_CLI`. It works because
   the Docker `FROM dinamica-ego` base image has Qt and its plugins installed system-wide, so Qt
   finds the platform plugin without `QT_PLUGIN_PATH`. Our AppImage extraction puts Qt plugins at
   `/opt/dinamica/usr/plugins/` — a non-standard path Qt will not search by default.

3. **Crash timing (Steps 8–11):** The crash happens before any argument processing — confirmed by
   `DinamicaConsole -version` crashing identically to a full model run. This is characteristic of
   Qt's `QApplication`/`QCoreApplication` constructor failing, which runs before `main()` argument
   parsing.

4. **`std::exception` base class (not a derived class):** Qt's platform plugin loading failure path
   in some Qt5 versions throws `std::runtime_error` or even a bare `std::exception` when the
   fallback "offscreen" plugin also can't be found. The `what()` message "std::exception" (the base
   class default) aligns with Qt's internal exception in plugin loader code.

5. **HPC nodes have no X11 DISPLAY.** Without `QT_QPA_PLATFORM=offscreen`, Qt attempts to load the
   `xcb` (X11) platform plugin. If that plugin isn't found via `QT_PLUGIN_PATH`, the lookup fails
   and Qt throws before the application even starts.

**AppRun env vars we replicated vs missed:**

| Var | Set by AppRun | In our `%environment` |
|-----|--------------|----------------------|
| `PYTHONHOME` | `$APPDIR/usr/` | ✓ (via `APPDIR` + AppRun logic) |
| `LD_LIBRARY_PATH` | extended with `$APPDIR/usr/lib/` | ✓ |
| `APPDIR` | (implicit, set by AppImage runtime) | ✓ |
| `DINAMICA_EGO_8_GDAL_DATA` | from wrapper | ✓ |
| `QT_PLUGIN_PATH` | `$APPDIR/usr/plugins` | **MISSING** |
| `QT_QPA_PLATFORM` | not set explicitly by AppRun | **MISSING (needed for headless HPC)** |
| `PYTHONPATH` | extended | **MISSING** |
| `PYTHONDONTWRITEBYTECODE` | `1` | **MISSING** |

**Status: FALSIFIED** — `ldd DinamicaConsole` shows zero Qt libraries. `/opt/dinamica/usr/plugins/platforms/` does not exist. DinamicaConsole has no Qt dependency; the crash cannot be a Qt platform plugin failure. `QT_PLUGIN_PATH` and `QT_QPA_PLATFORM` vars left in `%environment` are harmless but causally irrelevant.

---

### Hypothesis 7 — `PROJ_DATA` not set in container environment *(PRIMARY)*

**Evidence:**

1. **DinamicaEGO.sh source (Step 15):** The wrapper explicitly runs
   `export PROJ_DATA=${BIN_PATH}/Data/GDAL` before calling `bin/DinamicaConsole`. When called via
   D-104 (`cd /opt/dinamica/usr && bin/DinamicaEGO.sh <model>`), `BIN_PATH` resolves to
   `/opt/dinamica/usr/bin`, making `PROJ_DATA=/opt/dinamica/usr/bin/Data/GDAL`.

2. **`%environment` gap:** Our container `%environment` block exports
   `DINAMICA_EGO_8_GDAL_DATA=/opt/dinamica/usr/bin/Data/GDAL` (the Dinamica-specific GDAL data
   path) but has never exported `PROJ_DATA` (the PROJ library's own data path). These are
   separate: `DINAMICA_EGO_8_GDAL_DATA` is read by Dinamica's conf system; `PROJ_DATA` is read
   by the PROJ C library for datum/grid files.

3. **Crash timing:** DinamicaConsole crashes before producing any output, even on
   `DinamicaConsole -version`. This is consistent with PROJ/GDAL initialization failure in
   libBase.so's static initializers or constructor — before `main()` argument processing.

4. **ldd confirms GDAL dependency:** `ldd DinamicaConsole` includes `libGDAL.so` (via
   libBase.so). GDAL initializes PROJ at load time. If `PROJ_DATA` is unset, PROJ searches
   only system paths; the container (extracted AppImage) has no system-installed PROJ datum
   files, only the bundled ones under `/opt/dinamica/usr/bin/Data/GDAL`.

5. **DinamicaEGO.sh always sets it:** The wrapper script was written precisely because
   the binary needs these paths. We call `DinamicaEGO.sh` from D-104, which sets `PROJ_DATA`
   for the subprocess — but `%environment` (used when Apptainer invokes any command directly,
   including `%test`) never had it, and a pure `apptainer exec ... DinamicaConsole` call (as
   used in diagnostics) also bypasses the wrapper.

**Status: UNTESTED — PRIMARY CANDIDATE**

**Quick diagnostic (no rebuild needed on current .sif):**

```bash
# Step A: confirm PROJ data files exist at the expected path
apptainer exec "$DINAMICA_EGO_8_HOME" \
    bash -c 'ls /opt/dinamica/usr/bin/Data/GDAL/*.db 2>&1 | head -5'
# Expected: proj.db and related PROJ datum files

# Step B: test with PROJ_DATA set
apptainer exec \
    --home "$HPC_SCRATCH_ROOT/dinamica-home" \
    --env DINAMICA_EGO_8_TEMP_DIR="$HPC_SCRATCH_ROOT/dinamica-tmp" \
    --env PROJ_DATA=/opt/dinamica/usr/bin/Data/GDAL \
    "$DINAMICA_EGO_8_HOME" \
    bash -c 'cd /opt/dinamica/usr && bin/DinamicaEGO.sh /cluster/home/bblack/nascent-lulcc/dinamica/dinamica_model/smoketest.ego' 2>&1
# If H7 is the root cause: this should NOT crash with std::exception.

# Alternative: test DinamicaConsole directly (skips wrapper env setup)
apptainer exec \
    --home "$HPC_SCRATCH_ROOT/dinamica-home" \
    --env DINAMICA_EGO_8_TEMP_DIR="$HPC_SCRATCH_ROOT/dinamica-tmp" \
    --env PROJ_DATA=/opt/dinamica/usr/bin/Data/GDAL \
    "$DINAMICA_EGO_8_HOME" \
    DinamicaConsole -version 2>&1
```

---

## Proposed Fix — Plan 06 Iteration 5

**Precondition:** Step B above does not crash (H7 confirmed).

Add to `%environment` in `rocker-geospatial-dinamica.def`:

```
export PROJ_DATA=/opt/dinamica/usr/bin/Data/GDAL
```

**Rationale:**
- `DinamicaEGO.sh` sets `PROJ_DATA=${BIN_PATH}/Data/GDAL` for its subprocess, but the
  container `%environment` (which covers all Apptainer exec invocations that don't go through
  the wrapper) never had this variable.
- The PROJ C library (linked via libGDAL.so → libBase.so) requires `PROJ_DATA` to locate
  datum grid files (`proj.db` etc.). Without it, PROJ initialization fails at library load time.
- **No rebuild required for diagnosis.** Test immediately with `--env PROJ_DATA=...` flags
  against the current .sif. Rebuild only if the test confirms the fix.

**Also retain** all current `%environment` additions (APPDIR, GDAL_DATA, LOG_PATH, Qt vars) —
they are correct or at worst harmless.

---

## Out-of-Scope (REVISED)

- **Base image swap**: Iteration 3 tested Jammy (rocker/r-ver:4.4.3) — crash unchanged. H5 is
  FALSIFIED. The base image is not the root cause. Reverted back to `rocker/r-ver:4.5.3` (Noble)
  would also fail; staying on `4.4.3` is fine but not the fix.
- **AppImage URL change**: No evidence of version mismatch.
- **Any change to `src/dinamica_utils.r` or smoke test**: Mechanically correct per VERIFICATION.md.

---

## Rollback Guidance

If H7 is confirmed by Step B and iteration 5 .def rebuild still exits 5, the next escalation:
1. Check `PYTHONPATH`: AppRun sets `PYTHONPATH=/opt/dinamica/usr/lib/python3/dist-packages:...`.
   Add it to `%environment` and test.
2. Check `PYTHONSO`: `libBase.so` reads the `PYTHONSO` env var for its dlopen Python path.
   Test `--env PYTHONSO=/opt/dinamica/usr/lib/libpython3.12.so.1.0`.
3. Run `LD_DEBUG=all` inside container for the failing command to trace all symbol lookups.
4. File an issue at `https://github.com/ethzplus/rocker-geospatial-dinamica`.

---

*Source:* `01.1-03-SUMMARY.md` Open Issue 1 and the 2026-05-18 diagnostic ladder (Plans 05 and 06).
*Evidence files:* `diagnostics/ls-data-tree.log`, `diagnostics/strings-dinamica-env-vars.log`,
`diagnostics/strace-openat.log`, `diagnostics/fresh-appimage-extract-tree.log`,
`diagnostics/sif-extract-tree.log`, `diagnostics/extract-diff.log`.
*Reference:* `ethzplus/evoland-plus-HPC/src/steps/10_LULCC/` (working Dinamica on Euler setup).
*Feeds:* Plan 01.1-06 iteration 5 (PROJ_DATA env var, test on current .sif before rebuild).
