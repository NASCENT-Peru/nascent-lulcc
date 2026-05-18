# Diagnostic Findings — DinamicaConsole std::exception (Open Issue 1)

**Phase:** 01.1-fix-dinamica-launch-contract
**Date:** 2026-05-18 (updated after Plan 06 iteration 6 — LD_PRELOAD fix CONFIRMED)
**Summary:** Root cause is **`__gnu_cxx::recursive_init_error`** thrown during
`DFF::LogManager::Instance()` static initialization in `libBase.so`. The cycle:
`LogManager::Instance()` [guard locked] → `LogHub` → `FilesystemHelper` →
`getNewTemporaryDirectory()` → `getContextualLog()` → `LogManager::Instance()` [RECURSIVE →
throws]. This is a code bug in Dinamica EGO 8's `libBase.so`, independent of environment,
configuration, or base image. All prior hypotheses (H1–H7) are **FALSIFIED**.

**Fix confirmed (no rebuild required for test):** LD_PRELOAD interceptor compiled on Euler host
(`/tmp/dinamica_init_fix.so`) intercepts `_ZN3DFF10LogManager8InstanceEv` and
`_ZN3DFF16getContextualLogEv`. With fix active, `DinamicaConsole -version` prints the version
banner successfully:
```
[@0] Dinamica EGO 8, 8.11.2.20260408 (Kangaroo Kidney Pie) [build 787962234]
```
**Next step (Plan 07):** Bake the fix into the `.def` (compile in `%post`, export `LD_PRELOAD` in
`%environment`), rebuild `.sif`, run live smoke.

**Version note:** `.def` comment says `8.7.0.20250814`; actual installed binary (current `.sif`)
reports `8.11.2.20260408`. URL `https://dinamicaego.com/nui_download/1960/` serves the current
release, not a pinned version. The `smoketest.ego-decoded` `@version` line needs updating to
`8.11.2.20260408` before or during Plan 07.

---

## Evidence Summary

| Step | Evidence File / Source | Captured | Interpretation |
|------|------------------------|---------|----------------|
| 1 | `ls-data-tree.log` (316 lines) | `ls -R /opt/dinamica/usr/bin/Data/` + `ls -la /opt/dinamica/usr/bin/` | Data/GDAL intact; Data/R has Dinamica_1.0.8.tar.gz; all key binaries present |
| 2 | `strings-dinamica-env-vars.log` (0 bytes) | **EMPTY** — `strings` produced no output | Step 2 FAILED; cannot determine env var references directly |
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
| 16 | H7 direct test: `--env PROJ_DATA=/opt/dinamica/usr/bin/Data/GDAL DinamicaConsole -version` | **Still exits 5**: "Dinamica EGO exited with an error: std::exception" | H7 FALSIFIED — PROJ_DATA is not the root cause |
| 17 | `bash -x` trace of DinamicaEGO.sh | Wrapper sets PROJ_DATA, DINAMICA_EGO_8_LOG_PATH, DINAMICA_EGO_8_INSTALLATION_DIRECTORY correctly; D-104 `cd /opt/dinamica/usr` CWD confirmed | All wrapper-set env vars correct; cwd issue was a diagnostic artifact only |
| 18 | Exhaustive env var sweep: PYTHONHOME, PYTHONPATH, GDAL_DATA (absolute), DINAMICA_EGO_8_INSTALLATION_DIRECTORY (absolute), `--writable-tmpfs` | All still "Dinamica EGO exited with an error: std::exception" | No env var or filesystem writability combination resolves the crash |
| 19 | `LD_DEBUG=libs` inside container (sanity-checked with `LD_DEBUG=libs /bin/ls`) | Zero "error" or "not found" lines for DinamicaConsole; `LD_DEBUG=libs /bin/ls` confirmed LD_DEBUG works | Library loading is genuinely clean; crash happens after all libraries are mapped |
| 20 | `DinamicaConsole` with no args | Same "Dinamica EGO exited with an error: std::exception" | Crash is independent of arguments; occurs in startup code before any arg processing |
| 21 | `strings /opt/dinamica/usr/lib/libBase.so \| grep -E '/opt\|AppImage\|squashfs'` | No hardcoded absolute AppImage paths | Baked-in path hypothesis ruled out |
| 22 | LD_PRELOAD `__cxa_throw` hook (v1): intercepts all C++ exceptions | **`EXC_THROW: __gnu_cxx::recursive_init_error`** | True exception type revealed — NOT `std::exception` base; DFF::Exception (which doesn't override `what()`) wraps `recursive_init_error`; the "std::exception" display is the base-class default `what()` |
| 23 | LD_PRELOAD `__cxa_throw` hook (v2): full backtrace via `backtrace()` + `dladdr()` | **Full DFF call stack** (see H8 section below) | Root cause confirmed: circular singleton init in `DFF::LogManager::Instance()` ← `DFF::getContextualLog()` ← `DFF::FilesystemHelper::getNewTemporaryDirectory()` ← `DFF::FilesystemHelper::FilesystemHelper()` ← `DFF::FilesystemHelperManager::Instance()` ← `DFF::ApplicationUtils::getLogPath()` ← `DFF::LogHub::LogHub()` ← `DFF::LogManager::Instance()` [RECURSIVE] |
| 24 | `nm -D libBase.so \| grep getContextualLog`, `nm -D libBase.so \| grep LogManager` | `_ZN3DFF10LogManager8InstanceEv` (T), `_ZN3DFF16getContextualLogEv` (T) — both exported from libBase.so | Symbols available for LD_PRELOAD interposition |
| 25 | `DinamicaCoordinator --help` / `DinamicaCoordinator smoketest.ego` | Does NOT crash with recursive_init_error; exits with "Coordinator parse error: no positional args"; is a distributed-execution binary (needs --address, --port) | Coordinator takes a different initialization path (different `StaticUtils::initialize()` call or different singleton order); not a replacement for DinamicaConsole |
| 26 | `nm -D` across all `.so` files | `getContextualLog`: `T` in `libBase.so`, `U` in all plugin libs + DinamicaConsole binary | LD_PRELOAD interposition will intercept ALL callers automatically |
| 27 | `objdump -d libBase.so`: disassembly of `_ZN3DFF16getContextualLogEv` (48 bytes) | `call WorkerGroup::getCurrentWorker()` → if null → `call LogManager::Instance()@plt` + `add $0xc0,%rax` → `ret`; if non-null → `mov 0x258(%rax),%rax` → `ret` | Return type confirmed **`DFF::Log*`** (pointer, 8 bytes, returned in rax); no-worker path re-enters `LogManager::Instance()` = crash site |
| 28 | Call-site analysis: `0x8aae2c` + `0x8ab0ba` in `libBase.so` | `call getContextualLog()` → `mov %rax,%rdi` → `call canLogMessage(DFF::Log*, LogTag, LogSubsystem)` → `test %al,%al` → `je skip` | Return value passed to `canLogMessage()`; if `nullptr` → `canLogMessage` returns false → `je skip` taken → logging safely bypassed |
| 29 | LD_PRELOAD interceptor (`/tmp/dinamica_init_fix.so`) — first run | "PRELOAD: getContextualLog() during LogManager init: returning null" (×2) → "Failed to create temporary directory" | Fix fires (2 re-entrant calls intercepted); crash changes from `recursive_init_error` to a real runtime error about temp dir — initialization NOW COMPLETES |
| 30 | LD_PRELOAD interceptor — with `/tmp/dinamica-tmp` as temp dir | `[@0] Dinamica EGO 8, 8.11.2.20260408 (Kangaroo Kidney Pie) [build 787962234]` — **VERSION BANNER PRINTED, NO CRASH** | **Fix confirmed end-to-end**: DinamicaConsole starts successfully with LD_PRELOAD fix |
| 31 | Smoke test WITHOUT LD_PRELOAD (smoke_test_dinamica.sh --live) | exit 5: "Dinamica EGO exited with an error: std::exception" | Expected: smoke script's `apptainer exec` does not add `--env LD_PRELOAD=...`; fix must be baked into `.def` `%environment` to be transparent to all callers |

---

## Hypothesis Ranking

### Hypothesis 1 — Data/ subtree dropped during cp -a + rm -rf

**Status: FALSIFIED** — extract-diff.log zero `<` lines; all directories preserved.

---

### Hypothesis 2 — Missing DINAMICA_EGO_8_* environment variable

**Status: FALSIFIED** — B1 fix added GDAL_DATA and LOG_PATH to `%environment`; AppRun binary
(which sets the full env) also crashes directly. All env-var candidates tried exhaustively
(PYTHONHOME, PYTHONPATH, PROJ_DATA, GDAL_DATA, LOG_PATH, INSTALLATION_DIRECTORY, absolute paths).

---

### Hypothesis 3 — Specific file fails to open at startup

**Status: FALSIFIED** — LD_DEBUG=libs inside container showed zero file-open errors. All shared
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

### Hypothesis 6 — Qt platform plugin initialization failure (QT_PLUGIN_PATH not set)

**Status: FALSIFIED** — `ldd DinamicaConsole` shows zero Qt libraries.
`/opt/dinamica/usr/plugins/platforms/` does not exist. DinamicaConsole has no Qt dependency;
the crash cannot be a Qt platform plugin failure. `QT_PLUGIN_PATH` and `QT_QPA_PLATFORM` vars
left in `%environment` are harmless but causally irrelevant.

---

### Hypothesis 7 — `PROJ_DATA` not set in container environment

**Status: FALSIFIED** — Direct test with `--env PROJ_DATA=/opt/dinamica/usr/bin/Data/GDAL`
applied to the current `.sif` produced the same crash (step 16 above). While `DinamicaEGO.sh`
DOES set PROJ_DATA before calling DinamicaConsole, this env var is NOT the root cause.

---

### Hypothesis 8 — Circular singleton initialization in `DFF::LogManager::Instance()` *(ROOT CAUSE — CONFIRMED)*

**Evidence:**

The LD_PRELOAD `__cxa_throw` backtrace hook (steps 22–23) revealed the exact exception type
(`__gnu_cxx::recursive_init_error`) and the full DFF call stack:

```
DFF::StaticUtils::initialize()
  → DFF::LogManager::Instance()           [guard locked — first call, init begins]
    → DFF::LogHub::LogHub()
      → DFF::ApplicationUtils::getLogPath()
        → DFF::FilesystemHelperManager::Instance()
          → DFF::FilesystemHelper::FilesystemHelper()
            → DFF::FilesystemHelper::getNewTemporaryDirectory()
              → (2 private libBase.so functions)
                → DFF::getContextualLog()
                  → DFF::LogManager::Instance()  ← guard already locked → THROWS
```

**Why `std::exception` was displayed (not `recursive_init_error`):**
DinamicaConsole's catch block catches `std::exception` (the base class) and calls `e.what()`.
`DFF::Exception` — Dinamica's own exception class — does NOT override `what()`, so the base
class default `"std::exception"` is printed regardless of the derived type. The actual thrown
type (`__gnu_cxx::recursive_init_error`) was only visible via the `__cxa_throw` hook's
type-info extraction.

**Why `DinamicaEGO.sh` (GUI mode, no .ego arg) did NOT crash the same way:**
The GUI mode takes a different initialization path — `StaticUtils::initialize()` is either
deferred or calls a different ordering in the GUI entrypoint. The CONSOLE mode (any `.ego` arg
or `-version`) triggers `StaticUtils::initialize()` immediately.

**Why DinamicaCoordinator does NOT crash:**
DinamicaCoordinator uses a different subsystem initialization sequence that avoids the
`LogManager → LogHub → FilesystemHelper → getContextualLog → LogManager` cycle.

**Why the crash is base-image-independent (H5 FALSIFIED):**
The circular dependency is a CODE BUG in `libBase.so` (Dinamica's own library). It has nothing
to do with the Ubuntu base image version, glibc version, or libstdc++ version.

**Why LD_PRELOAD fixes it:**
1. Intercept `DFF::LogManager::Instance()` (`_ZN3DFF10LogManager8InstanceEv`): set TLS flag
   `logmanager_in_init=1` before calling the real function; return `nullptr` if called
   re-entrantly (when flag is already 1).
2. Intercept `DFF::getContextualLog()` (`_ZN3DFF16getContextualLogEv`): when
   `logmanager_in_init=1`, return `nullptr` immediately without calling `LogManager::Instance()`.
3. At the call site (step 28): `canLogMessage(nullptr, tag, subsystem)` returns `false` →
   the logging call is skipped safely → `getNewTemporaryDirectory()` continues without crash →
   `FilesystemHelper::FilesystemHelper()` completes → entire init chain completes.

**Status: CONFIRMED** — LD_PRELOAD interceptor fixes the crash. `DinamicaConsole -version`
prints the Dinamica version banner. Two PRELOAD intercept messages appear (two re-entrant
`getContextualLog()` calls during initialization are suppressed).

---

## Proposed Fix — Plan 07 (Bake LD_PRELOAD into container)

**Precondition:** LD_PRELOAD test confirmed (step 30 — `DinamicaConsole -version` prints banner).

### Part A: Add source file to repo

Commit `dinamica/container/dinamica_init_fix.cpp` with the following content:

```cpp
// LD_PRELOAD fix for Dinamica EGO 8 libBase.so circular singleton init bug.
// Root cause: DFF::LogManager::Instance() init -> LogHub -> FilesystemHelper ->
// getNewTemporaryDirectory -> getContextualLog -> LogManager::Instance() [RECURSIVE]
// -> __gnu_cxx::recursive_init_error.
// Fix: TLS flag tracks init state; getContextualLog() returns nullptr on re-entry
// so canLogMessage(nullptr,...) returns false and the logging call is safely skipped.
#define _GNU_SOURCE
#include <dlfcn.h>
#include <cstdio>

typedef void* (*VoidFunc)();
static __thread int logmanager_in_init = 0;

extern "C" {

void* _ZN3DFF10LogManager8InstanceEv() {
    static VoidFunc real = nullptr;
    if (!real) {
        real = reinterpret_cast<VoidFunc>(
            dlsym(RTLD_NEXT, "_ZN3DFF10LogManager8InstanceEv"));
        if (!real) return nullptr;
    }
    if (logmanager_in_init) return nullptr;
    logmanager_in_init = 1;
    void* result;
    try { result = real(); } catch (...) { logmanager_in_init = 0; throw; }
    logmanager_in_init = 0;
    return result;
}

void* _ZN3DFF16getContextualLogEv() {
    if (logmanager_in_init) return nullptr;
    static VoidFunc real = nullptr;
    if (!real) {
        real = reinterpret_cast<VoidFunc>(
            dlsym(RTLD_NEXT, "_ZN3DFF16getContextualLogEv"));
        if (!real) return nullptr;
    }
    return real();
}

} // extern "C"
```

### Part B: Add compile step to .def `%post`

After Stage 5 (conf seed), add Stage 6:

```bash
# Stage 6 - compile circular-init LD_PRELOAD fix (H8 in diagnostics/FINDINGS.md)
g++ -shared -fPIC -std=c++17 -O2 \
    -o /usr/local/lib/dinamica_init_fix.so \
    /opt/dinamica/usr/bin/Data/../../../dinamica_init_fix.cpp \
    -ldl
```

Wait — the source file needs to be accessible during %post. The cleanest approach is to inline
the source as a heredoc in %post:

```bash
    # Stage 6 - LD_PRELOAD fix for circular singleton init (H8 — diagnostics/FINDINGS.md)
    cat > /tmp/dinamica_init_fix.cpp << 'FIXEOF'
[source code above]
FIXEOF
    g++ -shared -fPIC -std=c++17 -O2 \
        -o /usr/local/lib/dinamica_init_fix.so \
        /tmp/dinamica_init_fix.cpp -ldl
    rm /tmp/dinamica_init_fix.cpp
```

Alternatively (preferred for auditability): commit the source to
`dinamica/container/dinamica_init_fix.cpp` and use `%files` to copy it into the container:

```
%files
    dinamica/container/dinamica_init_fix.cpp /tmp/dinamica_init_fix.cpp
```

Then in `%post`:
```bash
    # Stage 6 - compile circular-init LD_PRELOAD fix (diagnostics/FINDINGS.md H8)
    g++ -shared -fPIC -std=c++17 -O2 \
        -o /usr/local/lib/dinamica_init_fix.so \
        /tmp/dinamica_init_fix.cpp -ldl
    rm /tmp/dinamica_init_fix.cpp
```

### Part C: Add to `%environment`

```
export LD_PRELOAD=/usr/local/lib/dinamica_init_fix.so
```

This makes the fix transparent to all `apptainer exec` invocations (D-104 launch, `%test`
block, and any direct `DinamicaConsole` calls).

### Part D: Update `smoketest.ego-decoded` version

The current `smoketest.ego-decoded` has `@version = 8.7.0.20250814`. The installed binary
reports `8.11.2.20260408`. Update the `@version` line and re-encode via
`process_dinamica_script(mode='encode', check=TRUE)` before the rebuild.

**The `.def` comment block also needs updating** to reflect the actual installed version.

### Rationale

- `DinamicaEGO.sh` calls `DinamicaConsole` for its subprocess, so it also benefits from
  `LD_PRELOAD` being in `%environment`.
- `%test` calls `DinamicaConsole -version` directly — without `LD_PRELOAD` in `%environment`,
  the `%test` block would still crash. After this fix, `%test` should also pass.
- The fix intercepts only the two affected symbols and only during the single initialization
  window; normal operation after initialization is completely unaffected.
- The fix is idempotent: if a future Dinamica version fixes the circular init bug, the
  interceptors just forward to the real functions with zero overhead.

---

## Out-of-Scope (FINAL)

- **Base image swap**: Crash is OS-independent (H5 FALSIFIED). The base image is correct.
- **AppImage URL change**: No evidence of version mismatch being the issue. However, the
  URL `nui_download/1960/` is NOT pinned — it currently serves `8.11.2.20260408`. Update
  comments and `smoketest.ego-decoded` accordingly; consider pinning a specific URL in future.
- **Any change to `src/dinamica_utils.r` or smoke test script**: Not needed. The fix is
  entirely in the container — `LD_PRELOAD` in `%environment` is transparent to all callers.
- **Qt / PROJ_DATA / Python env vars**: All FALSIFIED hypotheses. Leave the harmless vars
  in `%environment` (they have no negative effect).

---

## Rollback Guidance

If the LD_PRELOAD fix lands in the rebuilt `.sif` but the smoke still exits 5:
1. Check `LD_PRELOAD` is set inside the container: `apptainer exec "$DINAMICA_EGO_8_HOME" bash -c 'echo $LD_PRELOAD'` — should be `/usr/local/lib/dinamica_init_fix.so`.
2. Check the `.so` was compiled: `apptainer exec "$DINAMICA_EGO_8_HOME" ls -la /usr/local/lib/dinamica_init_fix.so`.
3. Check the fix fires: `apptainer exec ... DinamicaConsole -version 2>&1 | grep PRELOAD` — should show the intercept message.
4. If fix fires but smoke still fails: examine the new error (it should NOT be `std::exception`
   — if it is, `LD_PRELOAD` is not being applied to the right binary; check if `DinamicaEGO.sh`
   spawns a subprocess that doesn't inherit `LD_PRELOAD`).
5. If smoke fails with a NEW error (not `std::exception`): that is a different runtime issue;
   investigate with the new error message.

---

*Source:* `01.1-03-SUMMARY.md` Open Issue 1 and the 2026-05-18 diagnostic ladder (Plans 05 and 06).
*Evidence files:* `diagnostics/ls-data-tree.log`, `diagnostics/strings-dinamica-env-vars.log`,
`diagnostics/strace-openat.log`, `diagnostics/fresh-appimage-extract-tree.log`,
`diagnostics/sif-extract-tree.log`, `diagnostics/extract-diff.log`.
*LD_PRELOAD fix source:* `/tmp/dinamica_init_fix.cpp` on Euler login node (to be committed as
`dinamica/container/dinamica_init_fix.cpp` in Plan 07).
*Feeds:* Plan 01.1-07 (bake LD_PRELOAD fix into .def, rebuild .sif, run live smoke).
