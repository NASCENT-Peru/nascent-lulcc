# Phase 5: Integrate spatial_interventions branch and stage intervention masks - Pattern Map

**Mapped:** 2026-09-22
**Files analyzed:** 20 (new, modified, moved)
**Analogs found:** 18 / 20

Line numbers refer to `main` at `262a3b2` unless marked `branch:` (= `git show origin/spatial_interventions:<path>`).

## File Classification

| New/Modified File | Role | Data Flow | Closest Analog | Match Quality |
|---|---|---|---|---|
| `src/implement_spatial_interventions.R` (merged from branch, then fix-ups) | service (DT transform) | transform (in-place DT) | `branch:src/implement_spatial_interventions.R` (the code itself) + `src/allocation.r` L683-692, L2539-2545 | exact (it is the merged file) |
| `src/allocation.r` — `generate_probability_maps()` hook + `year_post` param | service | transform | `src/allocation.r` L2470-2483, L2870-2884 | exact |
| `src/allocation.r` — `setup_allocation_inputs()` call site | service | request-response | `src/allocation.r` L2310-2324 | exact |
| `src/allocation.r` — `validate_allocation_runtime()` mask checks | middleware (pre-flight gate) | batch validation | `src/allocation.r` L392-507 | exact |
| `scripts/run_allocation.r` — `src_files` conflict | config (source list) | n/a | `scripts/run_allocation.r` L87-107 | exact |
| `config/hpc_config.yaml`, `config/local_config.yaml` — `interventions_dir` + conflict | config | n/a | `branch:` diff of both files | exact |
| `config/{BAU,NAT,CUL,SOC}_interventions.yml` (strip `spatial_masks/`, fix headers) | config | n/a | `branch:config/NAT_interventions.yml` L1-75 | exact |
| `.gitignore` conflict | config | n/a | both sides | trivial |
| `scripts/validate_intervention_masks.r` (NEW) | utility (standalone CLI validator) | file-I/O + batch | `scripts/diagnose_allocation_saturation.r` L22-108 (bootstrap/args) + `scripts/audit_transition_pipeline.r` L250-260 (PASS/FAIL exit) | role-match |
| `scripts/verify_intervention_smoke.r` (NEW, recommended) | utility (post-run assertion) | file-I/O | `scripts/verify_phase3_smoke.sh` L1-110 + `scripts/diagnose_allocation_saturation.r` | role-match |
| `tests/testthat/test-spatial-interventions.R` (NEW) | test (unit, tempdir raster fixture) | transform | `tests/testthat/test-saturation-diagnostics.R` L1-60 + `tests/testthat/test-allocation-lazy-predictor.R` L30-46 | exact |
| static assertions (add to `test-allocation-single-source-writer.R` or new file) | test (text-pattern) | n/a | `tests/testthat/test-allocation-single-source-writer.R` L10-81 | exact |
| pre-flight mask tests (extend `test-allocation-preflight.R` or in new test) | test | n/a | `tests/testthat/test-allocation-preflight.R` L21-70 | exact |
| `docs/spatial_interventions/scenario_narratives.md` (git mv + convert) | docs | n/a | `branch:scenario_narratives.txt` | content-port |
| `docs/spatial_interventions/spatial_masks.md` (git mv + convert) | docs | n/a | `branch:spatial_masks/README.txt` | content-port |
| `docs/spatial_interventions/urban_settlement_mask_methodology.md` | docs | n/a | `branch:spatial_masks/urban_settlement_mask_methodology.txt` | content-port |
| `docs/spatial_interventions/masks.sha256` (NEW) | config (manifest) | n/a | none (content given verbatim in RESEARCH §Mask Inventory) | no analog, content ready |
| `docs/README_HPC.md` — mask staging + intervention smoke section | docs | n/a | `docs/README_HPC.md` L278-297 ("Allocation smoke test") | exact |
| `src/old/` + `scripts/old/` moves (D-03) and doc callers (`docs/ARCHITECTURE.md`, `DEPLOYMENT.md`, `DEVELOPMENT.md`, `README.md`, `scripts/master_pipeline.sh`) | move / docs edit | n/a | `src/old/` (existing convention) | exact |
| `.planning/phases/05-.../05-MASK-VALIDATION.md` (validator output) | generated report | file-I/O | none (Markdown writer is new) | no analog |

---

## Pattern Assignments

### `src/implement_spatial_interventions.R` (service, transform on long DT)

**Base:** take the branch file whole (`branch:` L1-585). It replaces main's old-signature version. Nothing on main calls the old signature. Fix-ups go on top.

**Core loop to KEEP (branch L27-74):** YAML load, `Intervention_stage == "Allocation"` filter, `Time_steps_implemented` filter, ranking order with `na.last = TRUE`:
```r
Interventions <- yaml::yaml.load_file(file.path(
  interventions_dir,
  paste0(scenario, "_interventions.yml")
))
Current_interventions <- Interventions[sapply(Interventions, function(x) {
  x[["Intervention_stage"]] == "Allocation"
})]
Current_interventions <- Current_interventions[c(sapply(
  Current_interventions,
  function(x) simulation_time_step %in% x$Time_steps_implemented
))]
...
Current_interventions <- Current_interventions[order(
  sapply(Current_interventions, function(x) {
    if (is.null(x$Intervention_ranking)) NA else as.numeric(x$Intervention_ranking)
  }),
  na.last = TRUE
)]
```

**Class-name translation to KEEP (branch L86-94):** `class_name_to_value` comes from `load_allocation_class_map()`:
```r
Target_classes <- as.integer(
  class_name_to_value[unlist(intervention[["Transition_target_classes"]])]
)
if (any(is.na(Target_classes))) {
  stop(paste("Unknown class_name in Transition_target_classes:",
             paste(intervention[["Transition_target_classes"]], collapse = ", ")))
}
```

**Mask loading to REPLACE (branch L96-122):** `terra::rast(intervention[["Intervention_mask"]])` resolves relative to cwd, which is the D-05 anti-pattern. Replace it with the shared resolver (below) plus a per-call LUT cache. The Dynamic `is.null → next` branch at L107-117 stays only as defence. The resolver/pre-flight makes that case an error (D-14).

**Helper row-selection to KEEP; mask lookup to REPLACE (branch L247-259 absolute, L321-334 relative):**
```r
hit <- normalized$to_val == lulc_class
if (!is.null(From_filter_vals)) {
  hit <- hit & (normalized$from_val %in% From_filter_vals)
}
sub_idx <- which(hit)
if (length(sub_idx) == 0L) next
# REPLACE this:
mask_vals <- terra::extract(Intervention_mask, as.matrix(normalized[sub_idx, .(x, y)]))[, 1]
# WITH (RESEARCH Pattern 1):
inside_flag <- inside_lut[normalized$cell_id[sub_idx]]
```
Change the helper signature from `Intervention_mask` (a SpatRaster) to `inside_lut` (a logical vector indexed by region `cell_id`). Absolute Inside becomes `sub_idx[inside_flag]` and Outside becomes `sub_idx[!inside_flag]`, the same semantics as `!is.na(v) & v == 1`.

**LUT builder (NEW, RESEARCH Pattern 1).** Key the cache by file path and build it once per `implement_spatial_interventions()` call:
```r
.mask_inside_lut <- function(mask_path, cell_index, cache) {
  key <- normalizePath(mask_path, mustWork = TRUE)
  if (!is.null(cache[[key]])) return(cache[[key]])
  m <- terra::rast(mask_path)
  v <- terra::extract(m, cell_index$ref_cell_id)[[1L]]
  lut <- logical(max(cell_index$cell_id))
  lut[cell_index$cell_id[!is.na(v) & v == 1]] <- TRUE
  cache[[key]] <- lut
  lut
}
```
Use `cache <- new.env(parent = emptyenv())`, as in the per-from-class cache in `tests/testthat/test-allocation-lazy-predictor.R` L270. `cell_index` comes from `anterior_dt[, .(cell_id, ref_cell_id)]`. `ref_cell_id` is built at `src/allocation.r` L2585-2589:
```r
ref_grid <- terra::rast(config[["ref_grid_path"]])
anterior_dt[, ref_cell_id := terra::cellFromXY(ref_grid, cbind(x, y))]
data.table::setkey(anterior_dt, cell_id)
```

**NaN guard (NEW, insert after branch L364, before `Perc_diff > 0` at L415):**
```r
if (!any(Intervention_vals > 0) || !any(Non_Intervention_vals > 0) || !is.finite(Perc_diff)) {
  log_msg(sprintf("  intervention skip: to_val=%d no positive probabilities in one zone", lulc_class), log_file)
  next
}
```

**Logging: replace every `cat(...)` in the helpers (branch L237, L388, L418, L430, L445, L457, L486, L498, L513, L524, L545, L557) with `log_msg(..., log_file)`.** Add `log_file` to both helper signatures. See Shared Patterns / Logging.

**Resolver (NEW, same file so the validator and tests can source it without allocation.r).** Shape from RESEARCH Pattern 2. Reuse the YAML-read idiom from branch L28-31 and the fail-fast `stop(sprintf(...))` style used in `src/allocation.r` L576-585:
```r
resolve_intervention_masks <- function(interventions_dir, mask_dir, scenario, years) {
  f <- file.path(interventions_dir, paste0(scenario, "_interventions.yml"))
  if (!file.exists(f)) stop(sprintf("interventions YAML missing: %s", f))
  ivs <- yaml::yaml.load_file(f)
  # -> data.table(scenario, intervention_id, year, mask_name, mask_path, exists)
  # Static: scalar; Dynamic: [[as.character(year)]], NULL for an implemented year => stop()
  # reject grepl("[/\\\\]", mask_name) or ".." (D-05 bare filenames)
}
```

**Enum validation to KEEP (branch L120-121, L203-207, L310-317):** `stop(paste("Unknown Mask_type:", ...))` and `stop(paste("Unknown Prob_adjust_type:", ...))`.

**Trim reserved params:** `anterior` and `trans_rates_dt` (branch L19-20) are unused. Replace them with `cell_index`, `mask_dir` and `region_label` (discretion; RESEARCH recommends trimming).

---

### `src/allocation.r` — `generate_probability_maps()` hook (service, transform)

**Signature analog (L2470-2483).** Add `year_post` after `year_ant`:
```r
generate_probability_maps <- function(
  work_dir, region_label, region_val, scenario, year_ant,
  calibration_period, anterior_path, trans_rates_df, config,
  log_file, models_list, nhood_paths
) {
```
Also add `#' @param year_post` to the roxygen block above L2465.

**Hook site (L2870-2884).** After the merge, the branch hook lands on L2882-2883. The target shape:
```r
normalized[, tot_prob := sum(prob), by = cell_id]
normalized[tot_prob > 1, prob := prob / tot_prob]
normalized[, tot_prob := NULL]
data.table::setkey(normalized, row_idx)

# (replaces "#todo integrate more recent approach..." placeholder)
class_name_to_value <- load_allocation_class_map(config)
normalized <- implement_spatial_interventions(
  normalized           = normalized,
  cell_index           = anterior_dt[, .(cell_id, ref_cell_id)],
  class_name_to_value  = class_name_to_value,
  interventions_dir    = config[["interventions_dir"]],
  mask_dir             = config[["spat_prob_perturb_dir"]],
  scenario             = scenario,
  simulation_time_step = year_post,        # D-07 (branch had year_ant + step_length)
  log_file             = log_file,
  region_label         = region_label
)
data.table::setkey(normalized, row_idx)    # writer uses normalized[.(k)] at L2900
```
**Invariant:** the writer loop (L2891-2966) reads `normalized[.(k), nomatch = NULL]`. It needs the `row_idx` key and must not see `%03d` reordering. Do not touch L2885-2966.

**Class-map source (L683-692):**
```r
load_allocation_class_map <- function(config) {
  lulc_schema <- jsonlite::fromJSON(config[["lulc_aggregation_path"]], simplifyVector = FALSE)
  setNames(sapply(lulc_schema, function(x) x$value), sapply(lulc_schema, function(x) x$class_name))
}
```

### `src/allocation.r` — `setup_allocation_inputs()` call site (L2310-2324)

`year_post` is already a parameter (L2149). Add one line to the call:
```r
generate_probability_maps(
  work_dir = work_dir, region_label = region_label, region_val = region_val,
  scenario = scenario, year_ant = year_ant,
  year_post = year_post,                   # NEW
  calibration_period = calibration_period, anterior_path = anterior_path,
  trans_rates_df = trans_rates_df, config = config, log_file = log_file,
  models_list = models_list, nhood_paths = nhood_paths
)
```
Posterior years come from the pair construction at L1638-1665 (`year_ends <- tail(year_steps, -1)`, then `filter_allocation_timesteps()`). Do not reintroduce `step_length` (see the comment at L1608).

### `src/allocation.r` — `validate_allocation_runtime()` mask checks (pre-flight, batch)

**Analog: the same function, L392-507.** Put the mask checks only in the `else` (non-fixture) branch, inside `if (!is.null(config))` (L421-440), so fixture mode stays exact:
```r
if (!is.null(config)) {
  maybe <- function(key) { ... }
  ...
  files_expected <- c(maybe("ref_grid_path"), maybe("lulc_aggregation_path"), regions_json)
  # NEW: intervention masks (D-14)
}
```
**Error-line grammar (copy L470-475 and L454):** one `sprintf` per gap, appended to `errors`:
```r
for (f in files_expected) {
  if (!nzchar(f)) next
  if (!file.exists(f)) errors <- c(errors, sprintf("file: missing %s", f))
}
```
New lines: `sprintf("intervention mask: missing %s (scenario=%s id=%s year=%d)", ...)`. Years come from `tail(config$simulation_year_steps, -1)`, narrowed by `ALLOCATION_YEAR_POST_FILTER` (parse as in L570-578). Scenarios come from `config$scenario_names`. Guard with `exists("resolve_intervention_masks", mode = "function")`, because `implement_spatial_interventions.R` is sourced after `allocation.r`. Resolution happens at call time, so ordering is fine in production, but `test-allocation-preflight.R` sources only utils.r + allocation.r. Wrap resolver `stop()`s in `tryCatch` and turn them into error lines (`"intervention config: <msg>"`), so the list stays consolidated.

**Consumer (L1545-1552), unchanged:**
```r
preflight_errors <- validate_allocation_runtime(config = config)
if (length(preflight_errors) > 0L) {
  msg <- paste(c("Allocation pre-flight failed:", paste0("  - ", preflight_errors)), collapse = "\n")
  stop(msg, call. = FALSE)
}
```

---

### `scripts/run_allocation.r` — `src_files` (L87-107)

Current main:
```r
src_files <- c(
  "src/setup.r",
  "src/utils.r",
  "src/dinamica_utils.r",
  "src/saturation_diagnostics.r",
  "src/allocation.r"
)
```
Resolution: append `"src/implement_spatial_interventions.R"` (capital `.R`) after `"src/allocation.r"`. Drop the branch's `"src/lulcc.spatprobmanipulation.r"`. Keep the utils < saturation_diagnostics < allocation order (asserted in `test-allocation-single-source-writer.R` L70-81).

**Sourcing loop (L95-106)** swallows errors with `cat("ERROR sourcing ...")`. Recommended fix: after the loop, add
```r
stopifnot(exists("implement_spatial_interventions", mode = "function"))
```
or make the handler `quit(status = 1)`.

### Config YAMLs

**`config/{hpc,local}_config.yaml`: the branch adds this under `config_files_paths:` (resolved from project root by `build_full_config`):**
```yaml
  interventions_dir: "config"                          # directory containing the per-scenario *_interventions.yml files
```
Resolve the hpc conflict line to `  scenario_to_ssp_mapping:` (no trailing space). Keep `spat_prob_perturb_dir: "inputs/spat_prob_perturb"` (hpc L20, local L17) under `input_dirs` as is, since it is the D-05/D-08 mask root.

**`config/*_interventions.yml` (analog `branch:config/NAT_interventions.yml`)**, entry shape L44-75:
```yaml
- Intervention_stage: Allocation
  Intervention_ID: Conservation_expansion_and_preservation
  Intervention_ranking: 1
  Time_steps_implemented: [2024, ..., 2060]
  Transition_target_classes: [built_up_and_barren_lands, high_intensity_agricultural_areas, mining]
  Prob_adjust_type: Absolute
  Prob_adjust_value: 0
  Prob_adjust_zone: Inside
  Mask_type: Dynamic
  Intervention_mask:
    '2024': spatial_masks/protected_areas_mask_NAT_phase0_current.tif   # -> protected_areas_mask_NAT_phase0_current.tif
```
Edits: strip `spatial_masks/` from every `Intervention_mask` value (all 4 files). Header L9-10 cites the missing, gitignored `spatial_interventions_integration_protocol.md`: reword it. Header L12 `scenario_narratives.txt` becomes `docs/spatial_interventions/scenario_narratives.md`. Header L24 "Mask paths are relative to the project root." becomes "Mask entries are bare filenames resolved under `spat_prob_perturb_dir`."

---

### `scripts/validate_intervention_masks.r` (NEW, standalone CLI validator)

**Bootstrap analog: `scripts/diagnose_allocation_saturation.r` L22-61** (project-root setwd plus a `src_files` source loop):
```r
script_path <- commandArgs(trailingOnly = FALSE)
script_path <- script_path[grepl("--file=", script_path)]
if (length(script_path) > 0) {
  script_dir <- dirname(sub("--file=", "", script_path))
  project_root <- dirname(script_dir)
} else {
  project_root <- getwd()
  if (basename(project_root) == "scripts") project_root <- dirname(project_root)
}
setwd(project_root)

src_files <- c("src/setup.r", "src/utils.r", "src/implement_spatial_interventions.R")
```
For this script, make sourcing failure fatal (`quit(status = 2)`) instead of the swallowing handler.

**Arg parsing analog: `diagnose_allocation_saturation.r` L63-87** (`--flag value` and `--flag=value` while-loop, `stop(usage)` on unknown). Flags: `--out <report.md>`, optional `--data-root <path>` (local `data_basepath` is `E:/...`, but the data is on `D:`).

**Config load: `config <- get_config()` (`diagnose_allocation_saturation.r` L184, `audit_transition_pipeline.r` L72).** The reference raster is `terra::rast(config$ref_grid_path)`. Do not use `config$reference_crs`, which is a stale `epsg:2056`.

**Exit code analog: `scripts/audit_transition_pipeline.r` L250-260:**
```r
if (has_diff) {
  cat("AUDIT RESULT: FAIL -- set differences detected (see above)\n")
  quit(status = 1)
} else {
  cat("AUDIT RESULT: PASS -- all stages agree on transition set\n")
  quit(status = 0)
}
```
Checks per RESEARCH Pattern 3: `resolve_intervention_masks()` per scenario over all posterior years, `terra::compareGeom(r, ref, stopOnError = FALSE)` plus `nlyr == 1`, `terra::freq(r)$value %in% c(0, 1)`, orphans via `list.files(mask_dir, "[.]tif$")` (use `[.]`, not `\\.`), and non-tif items reported as INFO. Write the Markdown with `writeLines()`. There is no existing MD-writer helper in the repo.

### `scripts/verify_intervention_smoke.r` (NEW, post-run assertions)

**Analog: `scripts/verify_phase3_smoke.sh` L1-110.** Use its structure: resolve output paths through `get_config()`, then `simulation_output_dir/<SCENARIO>/<YEAR>/region_<suffix>/worker_logs`, then grep the region log for required markers (L67-78 loop), fail on forbidden markers (L80-85), and check that the posterior exists (L99-104). Output: one `PASS ...` line or `ERROR: ...` to stderr with `exit 1`. Region suffix rule: `gsub(" ", "_", tolower(region_label))` (`src/allocation.r` L2487). Required markers: 5x `AUDIT stage=intervention` for NAT 2032. Raster assertion: for each `%03d_id_trans_%d.tif` with `To*` in {104,105,106}, `global(r * (crop(mask, r) == 1), "max", na.rm = TRUE) == 0`. Writing it in R (bootstrap as the validator) is simpler than bash because it needs terra.

---

### `tests/testthat/test-spatial-interventions.R` (NEW, unit tests with a tempdir fixture)

**Header/sourcing analog: `test-allocation-lazy-predictor.R` L30-46.** Use `library(data.table)` plus `source()` into the global env. The file uses `:=`/`.()`, and the other tests' `new.env(parent = baseenv())` + `sys.source` pattern risks data.table's cedta check failing:
```r
library(testthat)
library(data.table)
.repo_root <- (function() {
  here <- tryCatch(normalizePath(sys.frame(1)$ofile %||% "."), error = function(e) ".")
  if (is.null(here) || identical(here, "")) here <- "."
  is_dir <- tryCatch(file.info(here)$isdir, error = function(e) NA)
  if (isTRUE(is_dir)) here <- file.path(here, "x")
  normalizePath(file.path(dirname(dirname(dirname(here)))), mustWork = FALSE)
})()
source(file.path(.repo_root, "src", "utils.r"))          # log_msg()
source(file.path(.repo_root, "src", "implement_spatial_interventions.R"))
```

**Inline raster fixture analog: `test-saturation-diagnostics.R` L23-45 and L57-65:**
```r
scratch <- withr::local_tempdir()
r <- terra::rast(matrix(as.vector(m), nrow = nrow(m), ncol = ncol(m)))
terra::writeRaster(r, file.path(scratch, "pa_mask.tif"), overwrite = TRUE, NAflag = -999)
```
Build a tiny "national" mask, a YAML via `yaml::write_yaml()` into `scratch`, a synthetic `normalized` (`row_idx, from_val, to_val, cell_id, x, y, prob`) and `cell_index` (`cell_id, ref_cell_id`). Pass `log_file = file.path(scratch, "t.log")` and assert on its `readLines()` for the AUDIT line. The case list is in RESEARCH §Validation Architecture.

### Static assertions (text-pattern tests)

**Analog: `test-allocation-single-source-writer.R` L18-25 (text load) + L70-81 (ordering via `regexpr`):**
```r
expect_match(run_script_text, '"src/implement_spatial_interventions.R"', fixed = TRUE)
expect_no_match(run_script_text, "lulcc.spatprobmanipulation.r", fixed = TRUE)
expect_match(allocation_text, "simulation_time_step = year_post", fixed = TRUE)
expect_no_match(allocation_text, "year_ant + config[[\"step_length\"]]", fixed = TRUE)
```

### Pre-flight test

**Analog: `test-allocation-preflight.R` L33-35 + L44-70.** Existing fixture tests must stay green (fixture branch untouched). For the new mask check, call `validate_allocation_runtime(config = list(interventions_dir = <tmp>, spat_prob_perturb_dir = <tmp>, scenario_names = "NAT", simulation_year_steps = c(2028L, 2032L)))` with a YAML that references a missing file. Assert `any(grepl("intervention mask: missing", result, fixed = TRUE))`. The resolver must be in scope (source `implement_spatial_interventions.R` into the same env), or the test goes in `test-spatial-interventions.R`.

---

### Docs (D-04, D-09)

- `git mv scenario_narratives.txt docs/spatial_interventions/scenario_narratives.md`, and similarly for `spatial_masks/README.txt` → `spatial_masks.md` and `spatial_masks/urban_settlement_mask_methodology.txt` → `urban_settlement_mask_methodology.md`. Then convert the content to Markdown headings/lists. Optionally move the tracked root `spatial_interventions_integration_explainer.md` here too.
- `docs/README_HPC.md`: add a "Spatial intervention masks" subsection next to "Allocation smoke test" (L278-297). Copy its style: a short prose lead, fenced `bash` blocks, and the `sbatch --export=ALL,...` form (L285-289). Add `--partition=highmem` (memory note: bare sbatch lands on `compute`, 93 GB). Content: the rsync canonical command, the scp fallback, `sha256sum -c docs/spatial_interventions/masks.sha256`, and `Rscript scripts/validate_intervention_masks.r`. All of it is verbatim in RESEARCH §HPC Placement.
- D-03 moves: `git mv` into the existing `src/old/` (convention: legacy files already live there, e.g. `src/old/simulation_trans_tables_prep.r`). For the two drivers, create a new `scripts/old/`, or use `src/old/` as D-03 says (planner choice). Update the text references at `docs/ARCHITECTURE.md` L57/L100/L117, `docs/DEPLOYMENT.md` L263/L311, `docs/DEVELOPMENT.md` L80/L133, `README.md` L109 and `scripts/master_pipeline.sh` L349.

---

## Shared Patterns

### Logging / AUDIT lines
**Source:** `src/utils.r` L1016-1024 (`log_msg`), `src/allocation.r` L2539-2545, `src/saturation_diagnostics.r` L290-331
**Apply to:** `implement_spatial_interventions.R` (every former `cat()`, plus one AUDIT line per intervention)
```r
log_msg(sprintf(
  "AUDIT stage=5 region=%s rate_table_rows=%d model_count=%d missing_model_id_trans=%s",
  region_label, n_rate_rows, n_model_rows, ...
), log_file)
```
New line: `AUDIT stage=intervention region=%s scenario=%s year=%d id=%s rank=%d type=%s zone=%s to_val=%d mask=%s rows_target=%d rows_changed=%d`. Use only `key=value` tokens so `grep "AUDIT stage=intervention"` works (see the saturation docstring L280-289).

### Fail-fast errors
**Source:** `src/allocation.r` L2166-2171, L2237-2248
**Apply to:** the runtime resolver when the YAML is missing or a class is unknown
```r
stop(log_msg(sprintf("Transition rate file not found: %s", trans_rate_src), log_file))
```
### Consolidated prerequisite list
**Source:** `src/allocation.r` L392-507 (collect into `errors`, never stop mid-loop)
**Apply to:** the pre-flight mask checks and the validator (collect FAIL rows, then one exit code at the end)

### Config path resolution
**Source:** `src/setup.r::build_full_config` (L260-333): `input_dirs` → `file.path(data_basepath, p)` (auto-`dir.create`d); `config_files_paths` → `file.path(find_project_root(), p)`
**Apply to:** all mask paths: `file.path(config[["spat_prob_perturb_dir"]], mask_name)`. YAML dir: `config[["interventions_dir"]]`. Check the files themselves, never just `dir.exists` (the directory always exists).

### Env-var smoke filters
**Source:** `src/allocation.r` L569-596 (`ALLOCATION_YEAR_POST_FILTER`), L549-567 (`ALLOCATION_REGION_FILTER`)
**Apply to:** year narrowing in the pre-flight, and the smoke-run commands

## No Analog Found

| File | Role | Data Flow | Reason |
|---|---|---|---|
| `docs/spatial_interventions/masks.sha256` | manifest | n/a | No checksum manifests in the repo. Content is verbatim in RESEARCH §Mask Inventory (14 lines) |
| `.planning/phases/05-.../05-MASK-VALIDATION.md` | generated report | file-I/O | No R script in the repo writes Markdown. Use plain `writeLines()` of pipe-table rows |

## Metadata

**Analog search scope:** `src/`, `scripts/`, `tests/testthat/`, `config/`, `docs/`, `origin/spatial_interventions` (branch file + diff vs `f09c064`)
**Files scanned:** ~20 read, ~90 listed
**Pattern extraction date:** 2026-09-22
