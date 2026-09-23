# Parameter Provenance for the Scenario Intervention Configs

_Closes review finding WR-11 (`05-REVIEW.md`) under decision D-16 of `05-CONTEXT.md`: record the
provenance in-repo rather than importing the external source._

## Why this file exists

The authoritative design source for `config/{BAU,NAT,CUL,SOC}_interventions.yml` is
`spatial_interventions_integration_protocol.md`, a working document held by the scenario author
(Manuel Kurmann) on his own machine. It is deliberately excluded from version control by
`.gitignore` line 50 (`spatial_interventions_integration_protocol.md`, under the
"spatial-interventions working / planning files (local-only)" rule), so the four YAML headers'
citations to its section 4a and section 3.7 cannot be followed by anyone reading this repository.
Without a substitute, no percentile, threshold, valency or zone in the shipped configs can be
reviewed or reproduced from the repository alone. This file is that substitute: it records where
each parameter's justification comes from, states the justification in terms of sources that *are*
versioned here, and names -- under `## Unresolved` -- the choices that genuinely still depend on the
external protocol. It paraphrases design intent already expressed in `docs/spatial_interventions/`
and in the YAML comments; it does not reproduce unpublished text from the protocol.

## Source of record

| Artifact | Status | Where it lives | Covers |
|---|---|---|---|
| `spatial_interventions_integration_protocol.md` | **Not versioned** (`.gitignore` line 50) | Scenario author's local machine (Manuel Kurmann) | Original design authority for every parameter choice; the section citations in the YAML headers point here |
| `docs/spatial_interventions/scenario_narratives.md` | Versioned (in repo) | This repository | The four scenario narratives (BAU/NAT/CUL/SOC) -- urbanisation shares, PA targets, mining regime, Indigenous land status. The clause each intervention encodes |
| `docs/spatial_interventions/spatial_masks.md` | Versioned (in repo) | This repository | What each mask *is* (inside = 1, outside = NA), its inputs and processing, and which intervention consumes it |
| `docs/spatial_interventions/urban_settlement_mask_methodology.md` | Versioned (in repo) | This repository | GHSL built-up threshold, clustering and 500 m buffer behind `urban_settlement_mask.tif` |
| `config/{BAU,NAT,CUL,SOC}_interventions.yml` headers and per-entry comments | Versioned (in repo) | This repository | Narrative anchor per scenario, ranking semantics, and the intent of each entry |
| `src/implement_spatial_interventions.R` (`relative_prob_adjust`, `absolute_prob_adjust`) | Versioned (in repo) | This repository | The *meaning* of `Prob_adjust_*` -- what a percentile, threshold, valency and zone actually do to a probability |

**Engine semantics used throughout the table below** (from `src/implement_spatial_interventions.R`):

- `Prob_adjust_type: Absolute` with `Prob_adjust_value: 0` sets the probability of the targeted
  transitions to exactly 0 in the named zone -- a hard block.
- `Prob_adjust_type: Relative` compares the mean probability *above* the given percentile inside the
  mask against the same statistic outside it, expresses the gap as a percentage difference
  (`Perc_diff`), and rescales only the above-percentile cells by `prob + (prob/100) * Perc_diff`.
  A percentile of `90` therefore means "act on the top decile of positive probabilities in each
  zone"; cells below that cut are left untouched, which is what makes every Relative intervention a
  soft, leaky constraint rather than a block.
- `Prob_adjust_threshold` is a floor on the absolute value of `Perc_diff`: when the two zones'
  top-decile means are within that percentage of each other, the adjustment is forced up to the
  threshold instead of dying out to nothing.

## Parameter rationale by intervention

Fourteen Allocation entries in total (BAU 1, NAT 5, CUL 5, SOC 3).

| Scenario | Intervention_ID | Prob_adjust_type | valency/value | zone | percentiles | threshold | Why these values |
|---|---|---|---|---|---|---|---|
| BAU | `ineffective_conservation` | Relative | Decrease | Inside | 90 / 90 | 5 | Narrative: "no expansion of conservation areas beyond the existing coverage of 17.88%" and planning "subject to capture by economic actors". Relative-Decrease (not Absolute=0) is the encoding of *ineffective* protection: only the top decile of conversion pressure inside existing PAs is damped, so lower-ranked conversions still leak through the designation. Mask is the static current-PA layer (`protected_areas_mask_BAU.tif`), matching "no expansion" |
| NAT | `Conservation_expansion_and_preservation` | Absolute | value `0` | Inside | n/a | n/a | Narrative: "strong push to expand conservation areas to cover 30% of the national territory by 2030", new areas managed as IUCN Ia/Ib where "human activities inside core areas are strictly managed". Strict protection plus enforcement is a hard block, so built-up / high-intensity-ag / mining probability is zeroed inside the mask. `Mask_type: Dynamic` with phase0 (17.9%, 2024-2028), then phase1 (~30%, 2032), then phase2 (full + connectivity, 2036-2060) encodes the *by-2030* timing rather than an instant switch |
| NAT | `Mining_freeze_post_2030` | Absolute | value `0` | Outside | n/a | n/a | Narrative: "no new concessions being granted beyond 2030, however existing operations are allowed to continue". The mask is the currently-titled concession footprint (`mining_concessions_mask.tif`, INGEMMET), so *outside* it new mining is impossible (hard 0) while *inside* it the fitted model probabilities are left untouched -- exactly "existing operations continue". `Time_steps_implemented` starts at 2032, the first 4-year step after 2030 |
| NAT | `Indigenous_land_OECM_forest` | Relative | Decrease | Inside | 90 / 90 | 5 | Narrative: Indigenous stewardship "formally recognised in the expansion of protected areas with the inclusion of indigenously managed reserves", but "stricter land-use regulations do occasionally create tensions where communities seek to maintain traditional agricultural or resource use practices". An OECM is a weaker instrument than a national park, so it is a soft top-decile decrease rather than a block. `From_lulc_filter: forested_areas` restricts it to forest loss, matching the mask's documented purpose in `spatial_masks.md` section 4 |
| NAT | `Indigenous_land_OECM_ag` | Relative | Decrease | Inside | 90 / 90 | 5 | Same OECM instrument, second documented transition of `indigenous_lands_mask.tif`: low-intensity to high-intensity agriculture. Narrative driver is NAT's shift to "agroforestry and agro-ecological farming", which makes production *more extensive*, so intensification inside titled community land is the thing being suppressed. Split from the forest entry because it needs a different `From_lulc_filter` and a single target class |
| NAT | `Urban_densification` | Relative | Increase_inside_decrease_outside | Inside | 90 / 90 | 5 | Narrative: 92% urban by 2060 "with people opting to live in compact settlements", plus "less arbitrary sealing of natural surfaces (soil) in urban areas". Demand is *not* reduced, it is relocated, so a one-sided push would not conserve the built-up total. The two-sided valency simultaneously boosts the top decile inside the settlement mask (GHSL built-up cores plus a 500 m growth-frontier ring) and suppresses the top decile outside -- the strongest available "concentrate growth into existing settlements" signal |
| CUL | `Conservation_expansion_and_preservation` | Absolute | value `0` | Inside | n/a | n/a | Narrative: Peru expands "the coverage of protected areas and Other Effective area-based Conservation Measures (OECMs) to 30% of the countries territory by 2030", selected for "sites of cultural heritage" and ecoregion representativeness. Same hard-block mechanism and same phase staggering as NAT; only the mask differs (`protected_areas_mask_CUL_phase*`, the Deleglise NaC prioritisation), because what changes between CUL and NAT is *where* the new estate goes, not how strictly it is enforced |
| CUL | `Mining_outside_restraint` | Relative | Decrease | Outside | 90 / 90 | 5 | Narrative: mining is "allowed to continue, however they are now strictly regulated to minimize their ecological footprint. New concessions can be granted, but strict legal requirements keep the actual exploitation of these sites relatively low". "New concessions can be granted" rules out NAT's Absolute=0 freeze; the encoding is continuous regulatory drag -- a top-decile Relative-Decrease outside the concession mask, active from 2024 because the regulation is in force from the start rather than switching on after 2030 |
| CUL | `Indigenous_land_OECM_forest` | Relative | Decrease | Inside | 90 / 90 | 5 | Narrative: titled Indigenous areas "are designated as Other effective area-based conservation measures (OECMs) with management responsibility devolved to communities", and CUL's Buen Vivir framing is explicitly Indigenous-led. Same instrument and same magnitudes as NAT; the CUL header records this as a known non-differentiation (`TODO: differentiate from NAT`), and the intended CUL-specific magnitudes are listed under `## Unresolved` |
| CUL | `Indigenous_land_OECM_ag` | Relative | Decrease | Inside | 90 / 90 | 5 | Second documented transition of the Indigenous mask (low- to high-intensity agriculture). CUL narrative: "holistic agro-ecological techniques such as agroforestry become widespread", with "the intensity of agricultural activities remaining fairly low", so intensification inside titled land is suppressed while total agricultural area is still allowed to expand moderately. Magnitudes shared with NAT pending the same TODO |
| CUL | `Urban_densification` | Relative | **Increase** (one-sided) | Inside | 90 / 90 | 5 | Narrative: "the increase in the proportion of the population living in urban areas is relatively small increasing from 78% to 81.5% by 2060, resulting in a greater area of lower density settlements", following "the desire of the population to dwell closer to nature". Boosting inside the settlement mask *without* the paired outside-decrease is the mechanical expression of "denser where settlement already is, but do not actively fence in the countryside" -- this single parameter is what separates CUL's urban logic from NAT's and SOC's |
| SOC | `Conservation_expansion_and_preservation` | Absolute | value `0` | Inside | n/a | n/a | Narrative: "the areal coverage of protected areas is expanded to 30% by the year 2030", new areas chosen to prioritise ecosystem services "such as water maintenance and carbon sequestration, as well as their potential to improve the connectivity of the national conservation estate", managed as IUCN category VI under "effective monitoring and enforcement". Hard block with the same phase staggering; the SOC-specific difference is the mask (`protected_areas_mask_SOC_phase*`, ES plus connectivity prioritised) |
| SOC | `Mining_in_low_ES_areas` | Absolute | value `0` | Outside | n/a | n/a | Narrative: "Additional mining concessions are granted, though these are limited to areas with comparatively lower values for ecosystem services and biodiversity". The permissive area is therefore not the legal concession footprint but `low_es_value_mask.tif` -- cells in the lowest NCP_baseline quartile *within their own modelling region* (`spatial_masks.md` section 5), so "low" is judged comparatively per region. Zeroing outside that mask is a zoning rule, not a moratorium: inside, new mining proceeds at model probability, reproducing "a steady rate of new site development". Active from 2024 because the EPR and zoning framework is in place from the start |
| SOC | `Urban_densification` | Relative | Increase_inside_decrease_outside | Inside | 90 / 90 | 5 | Narrative: "~92% of the population live in Urban areas by the year 2060", under landscape planning whose goal is "to maximise the efficiency of land use by clustering similar activities together (residential areas, agriculture, etc.)". Same urbanisation share and the same explicit clustering objective as NAT, so the same two-sided densification is used; SOC differs from NAT in *where new protected areas go*, not in how urban growth is steered |

## Non-obvious choices

### `Outside`-zone mining interventions, and the `Prob_adjust_value: 0` that goes with them

Three of the four scenarios constrain mining through the *complement* of a mask rather than its
interior, which reads backwards on first encounter. `Prob_adjust_zone: Outside` means the adjustment
is applied to cells **not** covered by the mask, leaving the masked area at the model's own
probability. For NAT's `Mining_freeze_post_2030` and SOC's `Mining_in_low_ES_areas` the pairing is
`Prob_adjust_type: Absolute` with `Prob_adjust_value: 0` -- a national ban on new mining *everywhere
except* inside the permitted footprint (titled concessions for NAT, low-ES cells for SOC). It is
emphatically not a ban inside the mask; inside, mining is left entirely alone. CUL's
`Mining_outside_restraint` uses the same `Prob_adjust_zone: Outside` geometry but a softer
mechanism: `Prob_adjust_type: Relative` with `Prob_adjust_valency: Decrease`, and no
`Prob_adjust_value` key at all, because CUL's narrative permits new concessions and only regulates
them. The consequence to keep in mind when reading these three entries: the mask names a *permitted*
area, and the parameters act on its complement.

> Correction to `05-REVIEW.md` WR-11: that finding describes "`Prob_adjust_zone: Outside` with
> `Prob_adjust_value: 0`" as belonging to CUL's `Mining_outside_restraint`. In the shipped config
> that combination belongs to NAT's `Mining_freeze_post_2030` and SOC's `Mining_in_low_ES_areas`;
> `Mining_outside_restraint` is Relative/Decrease and carries no `Prob_adjust_value` key. The
> parameters in this document are read from the YAML files as committed.

### `Prob_adjust_threshold: 5`

Every Relative intervention carries `Prob_adjust_threshold: 5`. This is not a magnitude -- it is a
floor. `relative_prob_adjust()` derives its adjustment from the percentage difference between the
top-decile mean probability inside the zone and outside it. When the model already sees the two
zones as statistically indistinguishable, that difference approaches zero and the intervention would
do nothing at all, silently. The threshold forces the absolute value of `Perc_diff` up to 5 in that
case (logged as "The Percentage difference is below the threshold ... setting to threshold value"),
so a configured intervention always applies at least a 5% nudge in its intended direction. It is the
guarantee that an intervention is never a no-op, not a tuning knob for strength.

### Soft Relative-Decrease, not Absolute=0, for BAU conservation

BAU's `ineffective_conservation` targets the same three high-impact destination classes as the NAT,
CUL and SOC conservation entries and covers the same kind of area (protected areas), yet uses
`Prob_adjust_type: Relative` with `Prob_adjust_valency: Decrease` where the other three use
`Prob_adjust_type: Absolute` with `Prob_adjust_value: 0`. The mechanism choice *is* the narrative
content. Because a Relative adjustment only touches cells above the 90th percentile and rescales
them proportionally, conversions below the cut are unaffected and high-pressure cells are reduced
rather than eliminated -- conversions keep happening inside Peru's protected areas. That leakage is
the modelled form of BAU's "planning remains largely reactive and subject to capture by economic
actors" and its weak enforcement of Indigenous and protected-area designations. Encoding BAU
conservation as a hard block would have made its protected areas behave exactly like NAT's, erasing
the scenario contrast the intervention exists to create.

## Unresolved

The following could **not** be reconstructed from in-repo sources and therefore still depend on the
external `spatial_interventions_integration_protocol.md`:

1. **The magnitude of the percentile cut.** Every Relative intervention uses
   `Prob_adjust_intervention_percentile: 90` and `Prob_adjust_non_intervention_percentile: 90`. No
   in-repo source explains why the top decile rather than the top quintile or the top 5%, and no
   sensitivity analysis over this value exists in the repository. The rationale rows above justify
   *that* a percentile cut is used (soft rather than hard constraint) but not the number 90.
2. **The value of `Prob_adjust_threshold: 5`.** The *role* of the threshold is derivable from
   `src/implement_spatial_interventions.R`; the choice of 5 rather than 2 or 10 is not recorded
   anywhere in the repository.
3. **Cross-scenario magnitude calibration.** All Relative interventions across all four scenarios
   share the identical `90 / 90 / 5` triple, so scenarios are differentiated only by mechanism,
   zone, mask and timing -- never by strength. Whether that uniformity is a deliberate design
   decision or an unfinished calibration is not recorded in-repo. The `TODO: differentiate from NAT`
   notes in the CUL and SOC headers (which propose, for CUL, raising `Prob_adjust_threshold` to 10
   or lowering the percentile for broader bite) suggest the latter, but the intended target values
   are not stated anywhere that is versioned.
4. **BAU's mountain-to-coast migration.** The BAU header states this is encoded via demand-side
   regional differences and the fitted models rather than a spatial intervention, and cites a
   protocol section for the rationale. `scenario_narratives.md` supports the *phenomenon*
   ("Disruption to water availability and quality in mountainous regions driven by substantial
   further decline of glacial areas forces migration of the population to the coast") but contains
   nothing about the modelling choice to handle it demand-side. That argument exists only in the
   protocol.
5. **BAU's protected-area share.** `scenario_narratives.md` is internally inconsistent for BAU: its
   Characteristics block lists "Protected areas (proportion of Peru under protection): 25% by 2030"
   while its Ecological Restoration and Protection section states "there is no expansion of
   conservation areas beyond the existing coverage of 17.88%". The BAU YAML header and
   `protected_areas_mask_BAU.tif` follow the 17.9% / no-expansion reading. Which figure the protocol
   intended, and therefore whether the BAU mask is the intended one, cannot be settled from in-repo
   sources.
6. **The protocol section mapping itself.** With the protocol outside the repository there is no way
   to verify that the cited sections are the ones that actually specify these entries, or that the
   protocol has not since been revised away from what is committed here.

## Authoring record

The four scenario YAMLs were first committed as `b01bb71` (2026-06-08, "Add per-scenario
intervention YAMLs and interventions_dir config"), authored from the protocol on 2026-06-05 per
their own headers. Their most recent content change is `5d5be7e` (2026-09-22, "refactor(05-03): bare
mask filenames and corrected headers in scenario YAMLs (D-05, D-06)"), which changed mask path form
and header text only; no `Prob_adjust_*` key or value has been modified since `b01bb71`.
