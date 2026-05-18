// LD_PRELOAD fix for Dinamica EGO 8 libBase.so circular singleton init bug.
// Root cause: DFF::LogManager::Instance() init -> LogHub -> FilesystemHelper ->
// getNewTemporaryDirectory -> getContextualLog -> LogManager::Instance() [RECURSIVE]
// -> __gnu_cxx::recursive_init_error.
// Fix: TLS flag tracks init state; getContextualLog() returns nullptr on re-entry
// so canLogMessage(nullptr,...) returns false and the logging call is safely skipped.
// See: .planning/phases/01.1-fix-dinamica-launch-contract/diagnostics/FINDINGS.md H8
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
