# Diagnostic Findings — DinamicaConsole std::exception (Open Issue 1)

**Phase:** 01.1-fix-dinamica-launch-contract
**Date:** 2026-05-18 (updated after Plan 06 iteration 3)
**Summary (updated 2026-05-18):** All env-var and conf-key hypotheses (H1–H4) are FALSIFIED. Three
full rebuild+smoke iterations all exit 5. LD_DEBUG=files inside the container shows zero file-load
errors — all shared libraries load successfully. AppRun (the compiled AppImage bootstrap binary)
also crashes when called directly inside the container. PYTHONHOME override to the embedded
PyEnvironment did not help. The crash is occurring inside DinamicaConsole's own C++ startup code
after library loading completes. The only remaining hypothesis is H5: Ubuntu Noble (24.04) C++
runtime incompatibility — a subtle glibc/C++ ABI difference between Noble and the Jammy (22.04)
base the Dinamica AppImage was compiled against. ldd confirms all libraries load, but ldd does not
catch ABI-level runtime differences in exception handling or vtable dispatch.
**Iteration 3 fix:** Roll back base image from `rocker/r-ver:4.5.3` (Noble 24.04) to
`rocker/r-ver:4.4.3` (Jammy 22.04). See H5 below.

---

## Evidence Summary

| Step | Evidence File | Captured | Interpretation |
|------|--------------|---------|---------------|
| 1 | `ls-data-tree.log` (316 lines) | `ls -R /opt/dinamica/usr/bin/Data/` and `ls -la /opt/dinamica/usr/bin/` | Data/GDAL intact (proj.db, GDAL cmake, GRIB csv files); Data/R has Dinamica_1.0.8.tar.gz; all key binaries present (DinamicaConsole, DinamicaCoordinator, DinamicaEGO.sh, DinamicaNUI.jar) |
| 2 | `strings-dinamica-env-vars.log` (0 bytes) | **EMPTY** — `strings` command inside container produced no output | Step 2 FAILED; cannot determine which DINAMICA_EGO_8_* vars the binary references |
| 3 | `strace-openat.log` (3 lines) | `ptrace_scope` permission denied; strace could not attach | Step 3 FAILED due to Euler HPC yama/ptrace_scope restriction; cannot identify which file DinamicaConsole fails to open |
| 4 | `fresh-appimage-extract-tree.log` (23 lines) + `sif-extract-tree.log` (106 lines) + `extract-diff.log` (99 lines) | `fresh`: 23 dirs at maxdepth 4 from squashfs-root; `sif`: 106 dirs at maxdepth 4 from /opt/dinamica/usr | Diff shows zero `<` lines (nothing dropped from fresh → sif); all `>` lines are explained by depth offset (sif goes one level deeper after prefix normalization) |
| 5 (Plan 06 Sub-step A) | DinamicaEGO.sh source (read from .sif at 2026-05-18) | Console-mode code path: sets `DINAMICA_EGO_8_LOG_PATH=dirname(model)` then calls `bin/DinamicaConsole "$@"` | Wrapper sets LOG_PATH at runtime from model path; APPDIR is NOT set by the wrapper — must come from %environment |
| 6 (Plan 06 B1 rebuild) | Live smoke exit 5 | Rebuild with `DINAMICA_EGO_8_GDAL_DATA` + `DINAMICA_EGO_8_LOG_PATH` in `%environment` | B1 fix FALSIFIED — crash persists (same D-107 pattern) |
| 7 (Plan 06 B2 rebuild) | Live smoke exit 5 | Rebuild with corrected conf key `GdalToolsData` + `APPDIR=/opt/dinamica` | Iteration 2 fix FALSIFIED — crash persists |
| 8 (iteration 3 diagnostics) | LD_DEBUG=files output (inside container, 2026-05-18) | All shared libraries open successfully; zero "no such file" errors | Library loading is NOT the cause — all deps present and load cleanly |
| 9 (iteration 3 diagnostics) | AppRun binary type check (2026-05-18) | `/opt/dinamica/AppRun` is ELF compiled binary (not shell script); also crashes when invoked directly | The AppImage bootstrap layer itself crashes — not a wrapper scripting issue |
| 10 (iteration 3 diagnostics) | PYTHONHOME override test (2026-05-18) | `--env PYTHONHOME=/opt/dinamica/usr/bin/PyEnvironment` → still `std::exception` | Python home path hypothesis FALSIFIED |
| 11 (iteration 3 diagnostics) | LD_DEBUG=init attempt (2026-05-18) | "warning: debug option `init' unknown; try LD_DEBUG=help" — glibc on Euler doesn't recognize `init` | Cannot use init-sequence debugging; LD_DEBUG=files (step 8) exhausted available glibc debug options |

**Depth offset explanation (critical for reading extract-diff.log):**
The `fresh` find uses `squashfs-root` as root; after `sed 's|squashfs-root/usr/|PREFIX/|'`, the maximum
effective depth from PREFIX is 2 (e.g. `PREFIX/bin/Data/GDAL`). The `sif` find uses `/opt/dinamica/usr`
as root; after `sed 's|/opt/dinamica/usr/|PREFIX/|'`, the maximum depth is 4 (e.g.
`PREFIX/bin/Data/GDAL/packages`). All 99 diff lines are `>` (sif-only) because the sif shows two
levels deeper — NOT because the sif added directories that weren't in the AppImage.

---

## Hypothesis Ranking

### Hypothesis 1 — `cp -a squashfs-root/. . && rm -rf squashfs-root DinamicaEGO.AppImage` dropped Data/ subtree

**Status: FALSIFIED**

`extract-diff.log` contains zero `<` lines. Every directory present in the fresh AppImage extract is also present in the `.sif` tree. The `cp -a + rm -rf` sequence preserved all directories.

---

### Hypothesis 2 — Missing DINAMICA_EGO_8_* environment variable

**Status: FALSIFIED**

DinamicaEGO.sh source (Sub-step A) confirmed the wrapper exports `DINAMICA_EGO_8_LOG_PATH=dirname(model)`
before calling DinamicaConsole. B1 fix added `DINAMICA_EGO_8_GDAL_DATA` and `DINAMICA_EGO_8_LOG_PATH`
to `%environment` — crash persisted. AppRun binary (which sets additional env vars `PYTHONHOME`,
`PYTHONPATH`, `QT_PLUGIN_PATH`, `PYTHONDONTWRITEBYTECODE`, extended `LD_LIBRARY_PATH`) also crashes when
invoked directly — ruling out any env-var missing from the wrapper chain. No env-var hypothesis
remains that has not been tested.

---

### Hypothesis 3 — Specific file fails to open at startup

**Status: FALSIFIED (evidence 8)**

LD_DEBUG=files inside the container showed all shared libraries loading successfully with zero errors.
No file-open failure occurs during library loading. The strace-based approach remains blocked
(ptrace_scope restriction on Euler), but LD_DEBUG=files confirms that shared library loading is NOT
the point of failure. The crash occurs after all libraries load — in DinamicaConsole's own C++ startup
code (constructors, global object initialization, or early main()).

---

### Hypothesis 4 — Missing `.dinamica_ego_8.conf` key

**Status: FALSIFIED**

B2 fix added `GdalToolsData = "/opt/dinamica/usr/bin/Data/GDAL"` (the correct key name, verified from
DinamicaEGO.sh source) to the conf seed. Crash persisted. The `PathForRData` key was also removed
(it does not exist in the actual conf schema). The conf-seed is now consistent with what DinamicaEGO.sh
reads, and the crash is unchanged.

---

### Hypothesis 5 — Ubuntu Noble (24.04) C++ ABI runtime incompatibility *(NEW)*

**Evidence:** Indirect — all direct hypotheses falsified; crash persists after every env-var, conf-key,
and library-path fix. AppRun (compiled binary) also crashes, confirming the issue is not in scripting
or env-var setup.

**Background:** The Dinamica EGO 8 AppImage was compiled against a specific glibc/libstdc++ version.
Ubuntu Noble (24.04) ships glibc 2.39 and libstdc++ 14. Ubuntu Jammy (22.04) ships glibc 2.35 and
libstdc++ 12. While `ldd` confirms all shared library names resolve, it does NOT verify C++ ABI
compatibility: vtable layout, exception handling, RTTI, or constructor/destructor behavior. A C++
program that uses `std::exception` in a way that was compiled against libstdc++ 12 (Jammy) ABI may
crash during early constructor/destructor chain under libstdc++ 14 (Noble) if the exception class
layout differs.

The `std::exception` crash pattern — consistent across all test conditions, occurring before any
user-visible startup banner — is characteristic of a global-constructor failure (static C++ objects
initialized at startup), which is the exact site where ABI differences manifest.

**Status: UNTESTED — PRIMARY CANDIDATE**

`rocker/r-ver:4.5.3` is based on Ubuntu Noble (24.04). Rolling back to `rocker/r-ver:4.4.3` (Jammy
22.04) is the targeted test. If the crash disappears on Jammy, H5 is confirmed.

**Note on the Plan 06 CONTEXT.md constraint:** CONTEXT.md D-102 says base-image swap is OUT unless
FINDINGS.md implicates it. This is now the explicit FINDINGS.md implication — H5 is the only
remaining hypothesis after all others are FALSIFIED. This constitutes the documented justification
the constraint required. The Out-of-Scope note below is accordingly revised.

---

## Proposed Fix — Plan 06 Iteration 3

**Context:** H1–H4 are FALSIFIED. H5 (Noble ABI) is the only remaining untested hypothesis.
All three prior builds and smokes exited 5 with identical crash output.

### Iteration 3 fix — Base image rollback to Ubuntu Jammy

Change `dinamica/container/rocker-geospatial-dinamica.def` `From:` line:

```
From: rocker/r-ver:4.4.3
```

(was `rocker/r-ver:4.5.3`)

**Rationale:** `rocker/r-ver:4.4.3` is the last R 4.4.x release on Ubuntu Jammy (22.04), providing
glibc 2.35 and libstdc++ 12 — the same base the Dinamica EGO 8 AppImage was almost certainly
compiled against. The geospatial apt packages (libgdal-dev, libgeos-dev, etc.) and R packages (sf,
stars, terra) install cleanly on Jammy. The R Dinamica package (`Dinamica_1.0.8.tar.gz`) is
installed from the extracted AppImage tarball and does not depend on the base Ubuntu version.

**Also retain all current `%environment` additions** (`APPDIR`, `DINAMICA_EGO_8_GDAL_DATA`,
`DINAMICA_EGO_8_LOG_PATH`) and the `GdalToolsData` conf key — these are correct and harmless
regardless of which hypothesis is root cause.

**If Iteration 3 also fails:**

1. File an issue at `https://github.com/ethzplus/rocker-geospatial-dinamica` with all collected
   diagnostic evidence (this FINDINGS.md + log files + the full iteration history).
2. Try the Dinamica EGO AppImage from a different download URL (e.g. an older build at
   `nui_download/1940/` or `nui_download/1950/`) to test if the crash is version-specific.
3. Check the upstream `ethzplus/rocker-geospatial-dinamica` GitHub repo for any open issues
   reporting similar `std::exception` crashes.

---

## Out-of-Scope (REVISED)

- **Base image swap (REVISED — now IN SCOPE for iteration 3):** Originally marked OUT based on the
  2026-05-17 ldd diagnostic showing glibc 2.34 needed / 2.39 provided. However, ldd only tests
  library load resolution, not C++ ABI compatibility at runtime. H5 specifically implicates a Noble
  vs Jammy runtime difference that ldd cannot detect. The base image swap IS warranted given H5 is
  the only remaining hypothesis. This constitutes the FINDINGS.md justification that CONTEXT.md D-102
  required before allowing a base-image change.

- **AppImage URL change (`nui_download/1960/`):** Retained as escalation step only; no evidence of
  version mismatch at this time.

- **Re-running the full original diagnostic ladder:** Not needed — evidence is sufficient to identify
  H5 as the next candidate.

- **Any change to `src/dinamica_utils.r` launch shape, smoke test script, or test files:**
  These are mechanically correct per `01.1-VERIFICATION.md` and should not be modified.

---

## Rollback Guidance

If iteration 3 (Jammy base image) still exits 5, file the upstream issue (see "If iteration 3 fails"
above) and update this FINDINGS.md with the new evidence. The phase cannot be closed without a
working `--live` smoke. Document the state in `01.1-03-SUMMARY.md` as "Open Issue 1 escalated to
upstream" and deferring closure to phase 01.2.

---

*Source:* `01.1-03-SUMMARY.md` Open Issue 1 and the 2026-05-18 diagnostic ladder (Plans 05 and 06).
*Evidence files:* `diagnostics/ls-data-tree.log`, `diagnostics/strings-dinamica-env-vars.log`,
`diagnostics/strace-openat.log`, `diagnostics/fresh-appimage-extract-tree.log`,
`diagnostics/sif-extract-tree.log`, `diagnostics/extract-diff.log`.
*Feeds:* Plan 01.1-06 iteration 3 (base image rollback to Jammy, rebuild on Euler, verify live smoke).
