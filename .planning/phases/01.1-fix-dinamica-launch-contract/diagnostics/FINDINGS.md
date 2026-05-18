# Diagnostic Findings — DinamicaConsole std::exception (Open Issue 1)

**Phase:** 01.1-fix-dinamica-launch-contract
**Date:** 2026-05-18 (updated after Plan 06 iteration 4)
**Summary (updated 2026-05-18):** H6 (Qt platform plugin not found) is the active
hypothesis. All prior hypotheses (H1–H5) are FALSIFIED. The root cause: AppRun (the
AppImage bootstrap binary we bypass when calling DinamicaConsole directly) sets
`QT_PLUGIN_PATH` before the application starts. Without it, Qt cannot find its
platform plugin on a headless HPC node and throws `std::exception` during its own
initialization — before any argument processing occurs. Our `%environment` replicated
APPDIR, GDAL_DATA, and LOG_PATH from AppRun but missed `QT_PLUGIN_PATH` (and
`QT_QPA_PLATFORM=offscreen` for headless operation).
**Iteration 4 fix (no rebuild needed for diagnosis):** Test `--env QT_QPA_PLATFORM=offscreen
--env QT_PLUGIN_PATH=/opt/dinamica/usr/plugins` on the current .sif. If confirmed,
add both to `%environment` and rebuild.

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

**Status: UNTESTED — PRIMARY CANDIDATE**

**Quick diagnostic (no rebuild needed on current .sif):**

```bash
# Step A: verify Qt platform plugins are present in the container
apptainer exec "$DINAMICA_EGO_8_HOME" \
    bash -c 'ls /opt/dinamica/usr/plugins/platforms/ 2>&1'
# Expected: should list libqoffscreen.so and/or libqxcb.so etc.

# Step B: test with Qt offscreen platform + plugin path
apptainer exec \
    --home "$HPC_SCRATCH_ROOT/dinamica-home" \
    --env DINAMICA_EGO_8_TEMP_DIR="$HPC_SCRATCH_ROOT/dinamica-tmp" \
    --env QT_QPA_PLATFORM=offscreen \
    --env QT_PLUGIN_PATH=/opt/dinamica/usr/plugins \
    "$DINAMICA_EGO_8_HOME" \
    DinamicaConsole --version 2>&1
# If H6 is the root cause: this should NOT crash.
# If still crashes: run Step C to get Qt plugin debug output.

# Step C: Qt plugin debug (if Step B still crashes)
apptainer exec \
    --env QT_DEBUG_PLUGINS=1 \
    --env QT_QPA_PLATFORM=offscreen \
    --env QT_PLUGIN_PATH=/opt/dinamica/usr/plugins \
    "$DINAMICA_EGO_8_HOME" \
    DinamicaConsole --version 2>&1 | head -80
```

---

## Proposed Fix — Plan 06 Iteration 4

**Precondition:** Step B above does not crash (H6 confirmed).

Add to `%environment` in `rocker-geospatial-dinamica.def`:

```
export QT_PLUGIN_PATH=/opt/dinamica/usr/plugins
export QT_QPA_PLATFORM=offscreen
```

**Rationale:**
- `QT_PLUGIN_PATH=/opt/dinamica/usr/plugins` — AppRun sets this to `$APPDIR/usr/plugins`; our
  flattened layout puts Qt plugins here. Without it, Qt searches system plugin paths and does not
  find the AppImage-bundled plugins.
- `QT_QPA_PLATFORM=offscreen` — Euler HPC nodes have no X11 DISPLAY. Setting this tells Qt to use
  the bundled offscreen platform plugin instead of attempting xcb (X11) initialization.
- **No rebuild required for diagnosis.** The above env vars can be tested immediately with
  `--env` flags against the current .sif. Rebuild only needed if the test confirms the fix.

**Also retain** all current `%environment` additions from prior iterations (APPDIR, GDAL_DATA,
LOG_PATH, GdalToolsData conf key) — these are correct and harmless.

---

## Out-of-Scope (REVISED)

- **Base image swap**: Iteration 3 tested Jammy (rocker/r-ver:4.4.3) — crash unchanged. H5 is
  FALSIFIED. The base image is not the root cause. Reverted back to `rocker/r-ver:4.5.3` (Noble)
  would also fail; staying on `4.4.3` is fine but not the fix.
- **AppImage URL change**: No evidence of version mismatch.
- **Any change to `src/dinamica_utils.r` or smoke test**: Mechanically correct per VERIFICATION.md.

---

## Rollback Guidance

If H6 is confirmed by Step B and iteration 4 .def rebuild still exits 5, the next escalation:
1. Run `QT_DEBUG_PLUGINS=1` (Step C above) to identify the exact plugin the system fails to load.
2. Check PYTHONPATH: AppRun also sets PYTHONPATH. Add `PYTHONPATH=/opt/dinamica/usr/lib/python3/dist-packages:...` if Qt debug shows Python-related failure.
3. File an issue at `https://github.com/ethzplus/rocker-geospatial-dinamica`.

---

*Source:* `01.1-03-SUMMARY.md` Open Issue 1 and the 2026-05-18 diagnostic ladder (Plans 05 and 06).
*Evidence files:* `diagnostics/ls-data-tree.log`, `diagnostics/strings-dinamica-env-vars.log`,
`diagnostics/strace-openat.log`, `diagnostics/fresh-appimage-extract-tree.log`,
`diagnostics/sif-extract-tree.log`, `diagnostics/extract-diff.log`.
*Reference:* `ethzplus/evoland-plus-HPC/src/steps/10_LULCC/` (working Dinamica on Euler setup).
*Feeds:* Plan 01.1-06 iteration 4 (Qt env vars, test on current .sif before rebuild).
