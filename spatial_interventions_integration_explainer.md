# Integrate `implement_spatial_interventions` into `allocation.r`

## Context

The project's allocation pipeline (`src/allocation.r`) predicts per-cell transition
probabilities from fitted tidymodels workflows, normalizes them, and hands them
to Dinamica EGO as per-transition probability rasters. Those probabilities
currently reflect **only** the statistical model — they don't carry any
scenario-specific spatial policy (e.g. "push urban growth *inside* building
zones under BAU", "suppress agricultural expansion inside protected areas under
NAT").

A standalone script, [`src/implement_spatial_interventions.R`](src/implement_spatial_interventions.R),
was developed in a spinoff project to apply such policies by reweighting
probabilities over spatial masks. It is **not currently called** from anywhere
in this repo. The task is to integrate it between probability prediction and
the per-transition TIF writes inside [`generate_probability_maps()`](src/allocation.r#L457).

The goal of this plan is to (1) brief a colleague on what the function
mechanically does, and (2) specify the concrete integration work.

---

## Mechanism — how the function works (colleague briefing)

### One-line summary

For each active intervention in the scenario's YAML, multiply/override the
model-predicted transition probabilities of chosen target classes within (or
outside) a spatial mask, so that scenario narratives shape *where* transitions
land without changing *how much* total change occurs.

### Inputs

| Input                       | Role                                                                    |
| --------------------------- | ----------------------------------------------------------------------- |
| Scenario's interventions YAML | Declarative spec of what interventions exist, when they fire, where they apply, and how they reweight probabilities |
| Current timestep (year)     | Gate — intervention only fires if year ∈ `Time_steps_implemented`       |
| Per-transition probabilities | The thing being modified (predicted by fitted models, already normalized) |
| Anterior LULC raster        | Needed for `From_lulc_filter` (restrict mask to cells currently in class X) |
| Spatial masks (TIFs)        | Defines the "intervention area" — building zones, protected areas, marginality, etc. |
| LULC schema                 | Maps class names ↔ raster integer values                                |

### Processing order (per scenario, per timestep)

1. **Load YAML** for current scenario. Keep only `Intervention_stage == "Allocation"` entries whose `Time_steps_implemented` includes this year.
2. **Sort by `Intervention_ranking`** (lower = earlier; `~`/NULL last). Earlier interventions affect probabilities that later interventions then further adjust — **order matters**.
3. **For each intervention**, in order:
   a. Load `Intervention_mask` (static TIF, or year-keyed dict for dynamic masks).
   b. If `From_lulc_filter` is set, AND the mask with cells whose current LULC class is in the filter (e.g. only cells currently `Urban` for `Agri_expansion`).
   c. Identify the target probability layers — `Prob_adjust_zone` (`Inside` or `Outside` the mask) × `Transition_target_classes` (which `To*` classes to adjust).
   d. Apply either:
      - **Absolute** adjustment → set prob to `Prob_adjust_value` for matching cells.
      - **Relative** adjustment → percentile-driven reweighting. Uses `Prob_adjust_valency` (Increase/Decrease), `Prob_adjust_intervention_percentile`, `Prob_adjust_non_intervention_percentile`, and `Prob_adjust_threshold` (min prob below which nothing is adjusted). Semantics of these are implemented in the helpers below.
4. **Result**: a modified probability table. Downstream per-transition TIFs are written from this modified table.

### Key YAML fields (cheat-sheet)

```yaml
- Intervention_stage: Allocation            # this script handles only Allocation-stage
  Intervention_ID: Urban_densification      # human-readable name, also used by hardcoded special-cases
  Intervention_ranking: 3                   # sort key; ~ = run last
  Time_steps_implemented: [2020, 2025, ...] # year gate
  Transition_target_classes: Urban          # which To* (destination class) probs to touch
  Prob_adjust_type: Relative                # Absolute | Relative
  Prob_adjust_valency: Increase             # Increase | Decrease (Relative only)
  Prob_adjust_intervention_percentile: 90   # (Relative only) — %ile inside zone
  Prob_adjust_non_intervention_percentile: 90
  Prob_adjust_threshold: 5                  # min prob (%) for adjustment to kick in
  Prob_adjust_zone: Outside                 # Inside | Outside (where to apply)
  Mask_type: Static                         # Static | Dynamic
  Intervention_mask: path/to/mask.tif
  From_lulc_filter: Urban                   # optional, restrict by current LULC class
```

### Peculiarities the colleague should know

- **Hardcoded special-case** (legacy script lines 160–184): if `Intervention_ID` is `Agri_maintenance` or `Agri_abandonment`, the mask is further restricted to the *upper quartile* of marginality values. This is a naming-based dispatch — fragile if renamed. The integration will preserve this behaviour but flag it as tech debt.
- **Only `Allocation` stage is implemented.** The YAML also defines `Pre-allocation` (patch-shape params) and `Post-allocation` (edit final LULC) stages — **out of scope for this task**, follow-up work.
- **Two helpers — `absolute_prob_adjust()` and `relative_prob_adjust()` — are called but not defined in this repo.** You (Ben) will supply these from the spinoff project before the colleague starts; the colleague should not implement them from scratch.
- **CRS / extent alignment is assumed, not checked.** Masks must already be on the same grid as the anterior LULC raster. Prep is a separate concern (spinoff has a `spatial_interventions_prep.r`).

---

## Required changes

### 1. Rename intervention configs to match scenario names

Current: `config/SSP{0,1,3,4,5}_interventions.yml` (5 files, SSP-named).
Project scenarios: `BAU`, `NAT`, `CUL`, `SOC` (4 scenarios, set in [`local_config.yaml:64`](config/local_config.yaml#L64)).

Target: `config/{BAU,NAT,CUL,SOC}_interventions.yml`.

**Open question for Ben before colleague starts**: which SSP corresponds to which
scenario? There are 5 SSP files and 4 scenarios — one will be dropped or
merged. Document the mapping in the file header comment once decided.

### 2. Add `interventions_dir` to both config files

- [`config/local_config.yaml`](config/local_config.yaml) and [`config/hpc_config.yaml`](config/hpc_config.yaml)
- Add under `config_files_paths:` (or a new `interventions_dir` key in the root) a path to the directory containing the YAML files. The project already has `config_dir: "config"` at the top; the cleanest option is to add:
  ```yaml
  interventions_dir: "config"
  ```
  and reference it via `config[["interventions_dir"]]` in R.
- Note: [`config/hpc_config.yaml:66`](config/hpc_config.yaml#L66) has a pre-existing indentation bug on the `scenario_to_ssp_mapping:` line (double indent) — fix while you're there.

### 3. Refactor [`src/implement_spatial_interventions.R`](src/implement_spatial_interventions.R)

**New signature** (to match what's available in `allocation.r`):

```r
implement_spatial_interventions <- function(
  normalized,            # long-format data.table: row_idx, from_val, to_val, cell_id, x, y, prob
  anterior,              # SpatRaster of current LULC (already loaded in generate_probability_maps)
  trans_rates_dt,        # data.table with From*, To*, id_trans, row_idx — needed to map target class names -> row_idx
  class_name_to_value,   # named int vector built from lulc_schema.json (class_name -> value)
  interventions_dir,     # config[["interventions_dir"]]
  scenario,              # e.g. "BAU"
  simulation_time_step,  # e.g. 2026
  log_file               # for log_msg(...) — project convention
)
```

Returns: the same `normalized` data.table with `prob` values modified in place
(or a new DT with the same schema).

**Changes from the legacy script:**

| Legacy                                                         | New                                                                              |
| -------------------------------------------------------------- | -------------------------------------------------------------------------------- |
| `raster_prob_values` wide df (x, y, LULC, Prob_<Class> cols)   | long-format `normalized` DT (see columns above)                                  |
| `LULC_rat` from Excel (`Class_abbreviation`, `Aggregated_ID`)  | `class_name_to_value` vector derived from [`config/lulc_schema.json`](config/lulc_schema.json) (fields `class_name` → `value`). Build once in `generate_probability_maps`, pass in. |
| `scenario_ID` (SSP name)                                       | `scenario` (BAU/NAT/CUL/SOC)                                                     |
| `Proj = ProjCH`                                                | Dropped — take CRS from `anterior` raster                                        |
| Wide-to-raster conversion via `rast(df[, c("x","y",col)])`     | Build per-target-class SpatRasters from `normalized` (rasterize from long rows) only when needed by the helpers, then write results back to `prob`. Alternatively, keep the helpers raster-native and do one round-trip per intervention; picking between these is a helper-implementation detail (your spinoff code will dictate). |
| `Current_lulc_raster` built from the wide df                   | Use the `anterior` SpatRaster directly                                           |
| `Transition_target_classes` matched via `paste0("Prob_", ...)` | Match target class string → integer via `class_name_to_value`, then filter `normalized` to rows where `to_val == <that integer>` |
| `From_lulc_filter` matched via `Class_abbreviation`            | Same — match string → integer via `class_name_to_value`                          |
| `cat(...)` for progress                                        | `log_msg(..., log_file)` — project convention ([`allocation.r:187`](src/allocation.r#L187)) |

Drop the Excel-based `LULC_rat` plumbing entirely. The JSON schema's
`class_name` values (e.g. `forested_areas`, `built_up_and_barren_lands`) become
the canonical names; **update the YAML files to use these** rather than the
legacy short codes (`Urban`, `Int_AG`, etc.). Include that find-and-replace in
the rename step (Step 1).

### 4. Wire the helpers

Ben will drop `absolute_prob_adjust()` and `relative_prob_adjust()` into
[`src/implement_spatial_interventions.R`](src/implement_spatial_interventions.R)
before the colleague starts. The colleague adapts them to operate on whichever
data shape the refactored wrapper settles on (long DT vs. per-target SpatRaster).

### 5. Integration point in `allocation.r`

Insert the call inside [`generate_probability_maps()`](src/allocation.r#L457)
**after the normalization block** (currently [`src/allocation.r:787-795`](src/allocation.r#L787-L795))
and **before the per-transition TIF-write loop** ([`src/allocation.r:805`](src/allocation.r#L805)):

```r
# after: normalized[, tot_prob := NULL]

# Build class_name -> value map once (reusing lulc_schema already loaded at L491)
class_name_to_value <- setNames(
  sapply(lulc_schema, function(x) x$value),
  sapply(lulc_schema, function(x) x$class_name)
)

normalized <- implement_spatial_interventions(
  normalized           = normalized,
  anterior             = anterior,
  trans_rates_dt       = trans_rates_dt,
  class_name_to_value  = class_name_to_value,
  interventions_dir    = config[["interventions_dir"]],
  scenario             = scenario,
  simulation_time_step = year_ant,  # or year_post — confirm with Ben
  log_file             = log_file
)
```

Also add `source("src/implement_spatial_interventions.R")` to the project's
loader (wherever `allocation.r`'s other sources are pulled in — check
[`src/main.r`](src/main.r) or the equivalent entrypoint).

### 6. Preserve the Agri_maintenance/abandonment special case

Carry over the upper-quartile-of-marginality mask restriction (legacy lines
160–184). Leave a `# TODO` noting that this should move to a YAML flag (e.g.
`Apply_marginality_quartile_filter: true`) to remove the hardcoded ID list.

---

## Files to modify

- [`src/allocation.r`](src/allocation.r) — insert call inside `generate_probability_maps()`.
- [`src/implement_spatial_interventions.R`](src/implement_spatial_interventions.R) — refactor signature + body per Section 3.
- [`config/local_config.yaml`](config/local_config.yaml) — add `interventions_dir: "config"`.
- [`config/hpc_config.yaml`](config/hpc_config.yaml) — same, plus fix existing indentation bug on line 66.
- `config/SSP{0,1,3,4,5}_interventions.yml` — rename to `{BAU,NAT,CUL,SOC}_interventions.yml` (mapping decision required), and replace short class names (`Urban`, `Int_AG`, etc.) with lulc_schema `class_name` values.
- Project loader / main entrypoint — `source()` the spatial interventions script.

---

## Verification

1. **Smoke run** — pick one scenario (BAU) and one small region (e.g. the smallest in `regions.json`), run `run_allocation_one_timestep()` for a single step. Confirm it completes end-to-end.
2. **Log check** — confirm the log for that region contains `"Applying intervention: <ID>"` messages for each active intervention, and no `"Unknown Prob_adjust_type"` / `"Unknown Mask_type"` errors.
3. **Differential check** — write per-transition TIFs twice: (a) with the new call enabled, (b) commented out. Load both into QGIS or terra and confirm the probability rasters for target classes differ inside (or outside) the intervention masks, per the YAML spec. Non-target transitions should be identical.
4. **Regression** — confirm rows with no active intervention for the current year pass through unchanged (compare `normalized$prob` before/after for a no-op scenario or no-op timestep).
5. **Scenario sweep** — run all four scenarios for one timestep and confirm the per-scenario YAML is actually being picked (log should print scenario name + filename).

---

## Open questions (resolve before handoff)

- [ ] Which SSP config maps to which scenario (BAU/NAT/CUL/SOC)? Which SSP is dropped?
- [ ] Should `simulation_time_step` be `year_ant` or `year_post`? (Legacy called it `simulation_time_step`; the YAMLs list years at 5-year spacing from 2020 — pick whichever matches that convention.)
- [ ] Confirm the refactored helpers (`absolute_prob_adjust`, `relative_prob_adjust`) will operate on the long-format DT — or whether they remain SpatRaster-native and the wrapper round-trips.
