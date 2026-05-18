# Diagnostic Findings — DinamicaConsole std::exception (Open Issue 1)

**Phase:** 01.1-fix-dinamica-launch-contract
**Date:** 2026-05-18 (updated after Plan 06 iteration 2)
**Summary (updated 2026-05-18):** B1+B2 fix (env vars + conf keys) FALSIFIED by live smoke still exiting 5.
DinamicaEGO.sh source (Plan 06 Sub-step A, 2026-05-18) reveals root cause: `APPDIR` is an
AppImage-runtime variable set to the squashfs mount path; DinamicaConsole uses it internally to
construct its Python home and other resource paths. Our .sif extracts squashfs-root to `/opt/dinamica`
— so `APPDIR=/opt/dinamica` is the correct fix. Also found: the correct conf key for GDAL data
is `GdalToolsData` (not `PathForGDALData` as originally inferred).
**Iteration 2 fix:** export `APPDIR=/opt/dinamica` in `%environment`; fix conf key to `GdalToolsData`.

---

## Evidence Summary

| Step | Evidence File | Captured | Interpretation |
|------|--------------|---------|---------------|
| 1 | `ls-data-tree.log` (316 lines) | `ls -R /opt/dinamica/usr/bin/Data/` and `ls -la /opt/dinamica/usr/bin/` | Data/GDAL intact (proj.db, GDAL cmake, GRIB csv files); Data/R has Dinamica_1.0.8.tar.gz; all key binaries present (DinamicaConsole, DinamicaCoordinator, DinamicaEGO.sh, DinamicaNUI.jar) |
| 2 | `strings-dinamica-env-vars.log` (0 bytes) | **EMPTY** — `strings` command inside container produced no output | Step 2 FAILED; cannot determine which DINAMICA_EGO_8_* vars the binary references |
| 3 | `strace-openat.log` (3 lines) | `ptrace_scope` permission denied; strace could not attach | Step 3 FAILED due to Euler HPC yama/ptrace_scope restriction; cannot identify which file DinamicaConsole fails to open |
| 4 | `fresh-appimage-extract-tree.log` (23 lines) + `sif-extract-tree.log` (106 lines) + `extract-diff.log` (99 lines) | `fresh`: 23 dirs at maxdepth 4 from squashfs-root; `sif`: 106 dirs at maxdepth 4 from /opt/dinamica/usr | Diff shows zero `<` lines (nothing dropped from fresh → sif); all `>` lines are explained by depth offset (sif goes one level deeper after prefix normalization) |

**Depth offset explanation (critical for reading extract-diff.log):**
The `fresh` find uses `squashfs-root` as root; after `sed 's|squashfs-root/usr/|PREFIX/|'`, the maximum
effective depth from PREFIX is 2 (e.g. `PREFIX/bin/Data/GDAL`). The `sif` find uses `/opt/dinamica/usr`
as root; after `sed 's|/opt/dinamica/usr/|PREFIX/|'`, the maximum depth is 4 (e.g.
`PREFIX/bin/Data/GDAL/packages`). All 99 diff lines are `>` (sif-only) because the sif shows two
levels deeper — NOT because the sif added directories that weren't in the AppImage.

---

## Hypothesis Ranking

### Hypothesis 1 — `cp -a squashfs-root/. . && rm -rf squashfs-root DinamicaEGO.AppImage` dropped Data/ subtree

**Evidence file:** `extract-diff.log` (all lines are `>`, not `<`) AND `ls-data-tree.log` lines 1–316.

**Status: FALSIFIED**

`extract-diff.log` contains zero `<` lines. Every directory present in the fresh AppImage extract
(`squashfs-root/usr/bin/Data/GDAL`, `squashfs-root/usr/bin/Data/R`,
`squashfs-root/usr/bin/PyEnvironment`, `squashfs-root/usr/bin/jre`, etc.) is also present in the
`.sif` tree. The `cp -a squashfs-root/. . && rm -rf squashfs-root DinamicaEGO.AppImage` sequence
in `dinamica/container/rocker-geospatial-dinamica.def` lines 87–89 preserved all directories.

`ls-data-tree.log` lines 5–203 confirm `Data/GDAL` is fully populated: `proj.db`, the full set
of GDAL cmake modules (lines 35–41), GRIB2 tables (lines 42–98), ITRF datum files (lines 115–118),
PROJ data files. `Data/GDAL/3.20`, `Data/GDAL/packages`, and `Data/GDAL/thirdparty` subdirs are
present (`sif-extract-tree.log` lines 10–12). `Data/R` has `Dinamica_1.0.8.tar.gz`
(`ls-data-tree.log` line 275).

**Conclusion:** The AppImage extract step did NOT drop the data subtree. H1 is definitively
falsified. No .def change to the `cp -a + rm -rf` sequence is warranted.

---

### Hypothesis 2 — Missing DINAMICA_EGO_8_* environment variable

**Evidence file:** `strings-dinamica-env-vars.log` (0 bytes — Step 2 FAILED).

**Cross-evidence:** `01.1-03-SUMMARY.md` lines 121–125 reports that `bin/DinamicaEGO.sh model.ego`
produces the same `std::exception` crash as `DinamicaConsole` called directly. The wrapper
(`bin/DinamicaEGO.sh`) is expected to export env vars (e.g. `DINAMICA_EGO_8_GDAL_DATA`,
`DINAMICA_EGO_8_LOG_PATH`, `PROJ_DATA`) before invoking DinamicaConsole. If the wrapper correctly
exports these vars to its DinamicaConsole subprocess AND the crash still occurs, then wrapper-set
env vars are not the root cause.

**Status: INCONCLUSIVE**

Step 2 failed (strings produced empty output — likely the binary uses internal string tables that
`strings` couldn't extract, or the command failed silently). We cannot confirm which vars the
binary references. The cross-evidence from `01.1-03-SUMMARY.md` (wrapper with `.ego` → same crash)
weakens H2 for vars the wrapper sets, but does NOT falsify H2 for vars that would need to come
from `%environment` (container-level) rather than the wrapper.

Specifically: if `DINAMICA_EGO_8_GDAL_DATA` or `DINAMICA_EGO_8_LOG_PATH` must be set at
container-level (not just wrapper-level), they are currently absent from `%environment` (current
`%environment` exports only `LD_LIBRARY_PATH`, `PATH`, `DINAMICA_EGO_8_INSTALLATION_DIRECTORY`,
`DINAMICA_EGO_8_HOME_DIR` — lines 113–116 of `rocker-geospatial-dinamica.def`). Adding them is
a low-risk, reversible change.

**Action for Plan 06 Task 1:** Read `DinamicaEGO.sh` to confirm whether it exports these vars
BEFORE calling DinamicaConsole: `apptainer exec "$DINAMICA_EGO_8_HOME" cat /opt/dinamica/usr/bin/DinamicaEGO.sh`.
If the wrapper exports them → H2 is weakened → escalate to H4.
If the wrapper does NOT export them → H2 is confirmed → add to `%environment`.

---

### Hypothesis 3 — Specific file fails to open at startup

**Evidence file:** `strace-openat.log` (3 lines — Step 3 FAILED).

```
strace: test_ptrace_get_syscall_info: PTRACE_TRACEME: Operation not permitted
strace: Could not attach to process. ...
strace: attach: ptrace(PTRACE_ATTACH, 2311469): Operation not permitted
```

**Status: UNVERIFIED**

Euler HPC has `/proc/sys/kernel/yama/ptrace_scope` set to a restrictive value (likely 1 or 2)
that prevents unprivileged attachment. Both the in-container strace and the host-strace
attachment attempt were blocked. The most informative single diagnostic remains inaccessible
without root access or a ptrace-unrestricted node.

This hypothesis cannot be confirmed or falsified with the current evidence. It remains possible
that DinamicaConsole opens a specific file at startup (a plugin manifest, a license file, a
registration database) that does not exist at the expected path inside the `.sif`.

**Alternative for Plan 06:** Without strace, the next best approach is to run DinamicaConsole
with an extended exception trace if it supports `--verbose`, `--debug`, or `DINAMICA_EGO_8_DEBUG`
env var. Check: `apptainer exec "$DINAMICA_EGO_8_HOME" DinamicaConsole --help 2>&1 | head -30`.

---

### Hypothesis 4 — Missing `.dinamica_ego_8.conf` key

**Evidence file:** None directly. Cross-reference with `dinamica/container/rocker-geospatial-dinamica.def`
`%post` Stage 5 (lines 97–104) and `src/dinamica_utils.r:resolve_dinamica_launch()` conf-seed block.

**Status: UNVERIFIED — PLAUSIBLE**

The `%post` Stage 5 seeds only four keys in `/root/.dinamica_ego_8.conf`:
```
AlternativePathForR = "/usr/local/bin/Rscript"
ClConfig = "0"
MemoryAllocationPolicy = "1"
RCranMirror = "https://cloud.r-project.org/"
```
(Lines 99–104 of `rocker-geospatial-dinamica.def`.)

DinamicaConsole may require additional configuration keys for its startup sequence. The four
seeded keys are administrative (R path, cluster config, memory policy, CRAN mirror). Path-related
keys that DinamicaConsole might look up at startup — such as a `PathForGDALData`,
`PathForRData`, or `PathForTemp` key — are absent from the seed.

**Note:** `bin/DinamicaEGO.sh` in GUI mode (no `.ego` arg) works correctly (`01.1-03-SUMMARY.md`
line 115–116). If GUI mode reads the conf too, and it works, then the conf is either not read in
GUI mode, or all currently-seeded keys are sufficient for GUI mode but NOT for console mode.
This asymmetry makes H4 plausible.

---

## Proposed Fix for Plan 06

**Context:** H1 is FALSIFIED. H3 is UNVERIFIED and inaccessible. H2 and H4 are the two remaining
testable candidates.

**Plan 06 Task 1 must execute in two sub-steps:**

### Sub-step A — Read DinamicaEGO.sh (required before modifying .def)

Run inside an `apptainer exec` to see the full wrapper source:

```bash
apptainer exec \
    --home "$HPC_SCRATCH_ROOT/dinamica-home" \
    --env "DINAMICA_EGO_8_TEMP_DIR=$HPC_SCRATCH_ROOT/dinamica-tmp" \
    "$DINAMICA_EGO_8_HOME" \
    cat /opt/dinamica/usr/bin/DinamicaEGO.sh
```

This is a read-only diagnostic — no build required. Look for:

1. Does the wrapper export `DINAMICA_EGO_8_GDAL_DATA`, `DINAMICA_EGO_8_LOG_PATH`, or `PROJ_DATA`
   before its DinamicaConsole invocation?
   - **If YES (wrapper exports them):** H2 for wrapper-level vars is FALSIFIED.
     Pivot to Sub-step B2 (H4 conf key extension).
   - **If NO (wrapper does NOT export them):** H2 confirmed. Apply Sub-step B1 (%environment fix).

2. Does the console-mode code path call `DinamicaConsole` directly or use a different launcher?

3. Is there a `--debug` or `--verbose` flag passed to DinamicaConsole in the wrapper?

### Sub-step B1 — Fix for H2 (missing env var): extend `%environment`

If DinamicaEGO.sh does NOT export `DINAMICA_EGO_8_GDAL_DATA` before calling DinamicaConsole,
modify `dinamica/container/rocker-geospatial-dinamica.def` `%environment` block (currently
lines 112–118) to add:

```
export DINAMICA_EGO_8_GDAL_DATA=/opt/dinamica/usr/bin/Data/GDAL
export DINAMICA_EGO_8_LOG_PATH=/tmp/dinamica
```

Provenance comment: `# Phase 1.1 gap-closure (Plan 06): add GDAL_DATA + LOG_PATH env vars — see diagnostics/FINDINGS.md.`

**Rationale:** `DINAMICA_EGO_8_GDAL_DATA` is referenced in `01.1-CONTEXT.md` D-104 as a var the
wrapper sets; its absence from `%environment` means bare DinamicaConsole invocations (and any
code path that bypasses the wrapper's env setup) would have it unset. `DINAMICA_EGO_8_LOG_PATH`
pointing to `/tmp/dinamica` (already created in `%post` line 110) prevents any log-file open
failure at startup.

### Sub-step B2 — Fix for H4 (missing conf key): extend `%post` Stage 5

If DinamicaEGO.sh DOES export the vars above (B1 is eliminated), or if B1 fix doesn't resolve
the crash, extend the conf seed at `rocker-geospatial-dinamica.def` lines 97–104 with path-related
keys. The exact keys depend on what DinamicaEGO.sh's console-mode code reveals, but the most
probable candidates are (add after line 102, before the `CONF` heredoc close):

```
PathForGDALData = "/opt/dinamica/usr/bin/Data/GDAL"
PathForRData = "/opt/dinamica/usr/bin/Data/R"
```

If these key names are wrong (they are inferred), DinamicaEGO.sh source or `strings DinamicaConsole`
output will reveal the correct ones. Provenance comment:
`# Phase 1.1 gap-closure (Plan 06): add data-path conf keys — see diagnostics/FINDINGS.md.`

**ALSO** apply the same keys to `src/dinamica_utils.r:resolve_dinamica_launch()` HPC branch
conf seeder (the `writeLines(...)` block at approximately lines 270–285 — grep for
`AlternativePathForR` to find it), so runtime-seeded staged-home conf is consistent with
the container-default.

### If both B1 and B2 fail

If the `.sif` rebuild and live smoke after B1 (and optionally B2) still exits 5, the next
escalation path is:

1. Check whether DinamicaConsole supports a `--verbose` or debug env var:
   `apptainer exec "$DINAMICA_EGO_8_HOME" DinamicaConsole --help 2>&1`
2. File an issue at `https://github.com/ethzplus/rocker-geospatial-dinamica` with the
   current diagnostic evidence.
3. Try pinning an older Dinamica AppImage version (earlier than `nui_download/1960/`) to
   see if the crash is version-specific.

---

## Out-of-Scope

The following fixes are NOT proposed and should NOT be implemented by Plan 06:

- **Base image swap** (e.g. `rocker/r-ver:4.5.3` → `rocker/r-ver:4.4.0`): The 2026-05-17 `ldd`
  diagnostic in `01.1-03-SUMMARY.md` lines 119–123 FALSIFIED the library-compat hypothesis.
  glibc 2.34 is needed, 2.39 is provided. All libraries map cleanly. Swapping the base image
  addresses a falsified hypothesis.
- **AppImage URL change**: `https://dinamicaego.com/nui_download/1960/` is the version used by
  the current install; changing it is only warranted if DinamicaEGO.sh reveals a version
  mismatch, which cannot be determined without reading the wrapper.
- **Re-running the full diagnostic ladder**: Plan 06 should act on the evidence collected here.
  If B1/B2 both fail, Plan 06's re-verify-and-iterate gate can re-run targeted diagnostics
  (specifically Sub-step A and `DinamicaConsole --help`) as part of a revised plan.
- **Any change to `src/dinamica_utils.r` launch shape, smoke test script, or test files**:
  These are mechanically correct per `01.1-VERIFICATION.md` and should not be modified.

---

## Rollback Guidance

If Plan 06's proposed fix lands in the `.def` and the rebuilt `.sif` still exits 5 (D-107
pattern caught by smoke test), Plan 06 re-runs its own diagnostic sequence:

1. Run Sub-step A (read DinamicaEGO.sh) if not yet done, to see the wrapper's console-mode
   code path.
2. Update this FINDINGS.md with updated hypothesis rankings.
3. Apply the next-ranked fix (B2 if B1 was tried, or escalation if both were tried).
4. Rebuild and re-verify.

A second iteration is expected if the first fix candidate is wrong — this is the standard
gap-closure iterate loop documented in `01.1-VERIFICATION.md`.

---

*Source:* `01.1-03-SUMMARY.md` Open Issue 1 (lines 104–138) and the 2026-05-18 diagnostic ladder.
*Evidence files:* `diagnostics/ls-data-tree.log`, `diagnostics/strings-dinamica-env-vars.log`,
`diagnostics/strace-openat.log`, `diagnostics/fresh-appimage-extract-tree.log`,
`diagnostics/sif-extract-tree.log`, `diagnostics/extract-diff.log`.
*Feeds:* Plan 01.1-06 (implement the fix and verify live smoke exits 0).
