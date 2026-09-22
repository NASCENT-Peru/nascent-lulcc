SPATIAL INTERVENTION MASKS
==========================

This folder holds the binary raster masks that
`src/implement_spatial_interventions.R` reads at allocation time to apply
scenario-specific spatial interventions to the per-transition probability
rasters.

Naming convention for finished masks: <intent>_<extent>.tif at the project's
100 m reference grid, in WGS 84. Each mask is value 1 = inside the
intervention area, NA = outside.

Status legend:
  [DONE]    Mask produced, documented, ready to reference from a YAML.
  [INPUT]   Raw vector or raster — not yet rasterized to the project grid.
  [PENDING] Discussion still open with collaborators / mask not yet produced.


===============================================================================
1. urban_settlement_mask.tif                                            [DONE]
===============================================================================

WHAT IT IS
  Binary mask of "settlement zones" — existing built-up cores in Peru plus a
  500 m growth-frontier ring around each. Used by Urban_densification in
  the NAT, CUL and SOC scenarios.

INPUT
  GHSL R2023A "Built-up surface per 100 m cell", 2025 epoch (modelled).
  Source: European Commission Joint Research Centre (JRC).
  File:   /Users/manuelkurmann/Desktop/ghs-pop/GHS_BUILT_S_2025_Peru.tif
  Shape:  24,192 x 16,579 cells (Peru extent, WGS 84, ~100 m).
  Values: m² of built-up surface per 100 m cell (0 to ~7,800).

PROCESSING
  Morphological clustering pipeline (raster equivalent of DBSCAN), 7 steps:
    1. Threshold     ghsl >= 1500 m² -> binary urban indicator
                     (280,604 cells = >=15% built-up).
    2. Dilate        iterated 3x3 max focal, 2 iterations (~250 m reach).
    3. Patches       aggregate to 200 m, terra::patches() 8-connected,
                     disaggregate back to 100 m. Result: 8,404 connected
                     components.
    4. Restrict      mask cluster IDs to the original urban footprint so
                     cluster size is counted in real built-up cells.
    5. Size-filter   keep clusters with >=10 original urban cells
                     (>=10 ha minimum). 1,579 of 8,404 clusters kept;
                     6,825 dropped as isolated buildings/sparse hamlets.
                     95% of input urban cells retained.
    6. Buffer        iterated 3x3 max focal, 5 iterations (~500 m ring)
                     around kept cluster footprints.
    7. Write         INT1U + DEFLATE/PREDICTOR=2/TILED.

  Parameters: threshold_m2 = 1500, eps_m = 250, min_cells = 10, buffer_m = 500.
  Final output: 1,069,101 mask cells, expansion factor 3.81x input urban.

  Characterisation of these parameters (numbers from the actual run):
    - The 1500 m² threshold corresponds to roughly the top 5% of GHSL's
      built-up-detected cells (280,604 of 5,675,545). At this density
      an urban cell is a contiguous block of adjacent buildings, not
      scattered rural houses.
    - 1,579 of 8,404 connected components pass the min_cells = 10
      filter. The 6,825 dropped clusters average ~2 cells each (single
      buildings or pixel-level noise). Despite filtering out 81% of
      cluster candidates, 95% of input urban cells are retained.
    - Average kept settlement footprint after the 500 m buffer:
      ~677 mask cells (~6.8 km²).
    - Spot check across 28 named locations: 22/22 settlements
      (major cities + small Andean and Amazon towns) inside the mask;
      0/6 wilderness checkpoints (Sechura desert, Atacama dunes,
      Cordillera Blanca peaks, Nazca Lines, two rural points) inside.

  Full methodology and per-parameter notes are documented in
  urban_settlement_mask_methodology.txt.

USED BY
  NAT, CUL, SOC: Urban_densification


===============================================================================
2. mining_concessions_mask.tif                                           [DONE]
===============================================================================

WHAT IT IS
  Binary mask of areas covered by currently-titled Peruvian mining
  concessions — the legal boundaries within which mining-LULC transitions
  are permitted. Used by NAT's freeze ("no new concessions after 2030") and
  CUL's softer "outside-restraint" reduction.

INPUT
  INGEMMET (Instituto Geológico, Minero y Metalúrgico) ArcGIS REST service,
  layer 0 "Catastro Minero" of the SERV_CATASTRO_MINERO_WGS84 MapServer.
    Endpoint: https://geocatminapp.ingemmet.gob.pe/arcgis/rest/services/
              SERV_CATASTRO_MINERO_WGS84/MapServer/0
    License:  Peruvian government open data (institutional).
    Download date: 2026-05-18.
    Downloaded vector:
      ~/Downloads/ingemmet_concesiones_peru.gpkg   (41 MB, 64,182 polygons)
      ~/Downloads/ingemmet_concesiones_peru.rds    ( 7 MB, R-native backup)

  Each polygon is a "cuadrícula minera" or contiguous block of cuadrículas.
  Peruvian mining law (D.S. 018-92-EM) requires all concessions to be
  rectangular blocks of 100-hectare quadrats axis-aligned to UTM
  coordinates, so polygons are rectangular by construction (most are
  5-vertex; 7-vertex L-shapes appear when two quadrats are joined).

  D_ESTADO field values (64,182 features total at download):
    Titled / active                 41,912  ← KEPT
      "D.M. Titulado D.L. 708"       37,236  modern mining law
      "D.M. Titulado D.L. 109"        4,461  older mining law (still valid)
      "Acumulación D.M. Titulada"       215  aggregated titles
    Pending applications            16,610  excluded
      "D.M. en Trámite D.L. 708"     16,608
      "D.M. en Trámite D.L. 109"          2
    Extinguished / suspended         3,375  excluded
    Operational sites                2,285  excluded (not concessions)
      Cantera (D.S. 037-96-EM)        1,887
      Planta de Beneficio               375
      Depósito de Relaves                23

  Caveats:
    - "Titled" is a legal status, not an extraction footprint. A 100-ha
      concession typically extracts only a small fraction. The mask is
      ~12.4% of Peru but actual mining LULC is far less — that's expected.
    - The cadastre is continuously updated; this is the snapshot as of
      2026-05-18.

PROCESSING
  Steps performed by build_mining_concessions_mask.R:
    1. Read GPKG via terra::vect().
    2. Filter via regex `D_ESTADO ~ "Titulad"` (captures all three titled
       variants without re-listing literal strings).
    3. CRS matches the project grid (WGS 84) — no reprojection needed.
    4. terra::rasterize(field = 1L, background = NA, touches = TRUE)
       onto the project's 100 m grid (template:
       spatial_masks/urban_settlement_mask.tif).
       touches=TRUE means any cell whose centre or boundary is touched by a
       polygon is marked 1; gives full coverage of even narrow concessions.
    5. Write INT1U + DEFLATE/PREDICTOR=2/TILED.

  Result: 20,136,257 mask cells, ~159,500 km² ≈ 12.4% of Peru's land area.

  Download recipe (used once to produce the GPKG that feeds this script):
    ~/Downloads/download_ingemmet_concessions.R
  That script documents quirks of the INGEMMET service (POST queries
  rejected, where=1=1 returns HTTP 500, URL-length cap on GETs, f=geoJSON
  unsupported despite docs claiming it is, R/httr's bundled libcurl can't
  connect to the host so the script shells out to system curl, etc.).

USED BY
  NAT: Mining_freeze_post_2030   (Prob_adjust_zone: Outside, Absolute = 0)
  CUL: Mining_outside_restraint   (Prob_adjust_zone: Outside, Relative-Dec)


===============================================================================
3. protected_areas_mask_*.tif  (10 files, time-staggered per scenario)   [DONE]
===============================================================================

WHAT THEY ARE
  Per-scenario protected-area intervention masks for the Conservation
  interventions in NAT / SOC / CUL (time-staggered to reflect the 17.9% ->
  30%-by-2030 expansion narrative) and the static current-PA mask for
  BAU's Conservation_existing_only / ineffective_conservation.

INPUT
  Outputs of the Peru 30*30 conservation-planning framework
  (Deléglise et al., 2024), shared as the folder
    spatial_masks/Results_NASCENT_priorities_protected_areas_deleglise/
  which contains, per NASCENT scenario:
    Final_results_BaU.tif  Final_results_NfN.tif
    Final_results_NfS.tif  Final_results_NaC.tif
  plus the methodology PDF (report_scenarios.pdf) and a per-scenario
  Excel of optimization weights (PA_expansion_inputs_final.xlsx).

  Source rasters:
    Shape:  204 x 141 cells.
    CRS:    UTM zone 18S (EPSG:32718).
    Cell:   10 km.
    Values: -1 = unprotected / background
             1 = Current PAs (Peru's 17.9% baseline, 2023)
             2 = Potential PAs Step 1 (reach scenario target by 2030)
             3 = Potential PAs Step 2 (post-2030 connectivity push)

  Per-scenario optimization weights (from the Excel, for provenance only —
  not used programmatically in the build):
                       Biod  Carb  Wat  HumC  Tour  PAtgt%  EcReg%  MinSize  Indig
    NAT (NfN)            2     1    1   -3    -2    30%     17%     20 km    0.30
    SOC (NfS)            1     2    2   -1     1    30%     17%     20 km    0.60
    CUL (NaC)            1     1    1    0     2    30%     10%     10 km    0.75
    BAU                  1     1    1   -2     1    25%     10%     10 km    0.00

  Citation:
    Deléglise, H., Justeau-Allaire, D., Mulligan, M., Espinoza, J. C.,
    Isasi-Catalá, E., Alvarez, C., ... & Palomo, I. (2024).
    Integrating multi-objective optimization and ecological connectivity to
    strengthen Peru's protected area system towards the 30* 2030 target.
    Biological Conservation, 299, 110799.

PROCESSING
  build_protected_areas_masks.R does, per scenario:
    1. Reclassify the categorical source raster to a binary mask, keeping
       a different subset of categorical values depending on the phase
       (see table below).
    2. terra::project(method = "near") from UTM 18S 10 km to the project's
       WGS 84 100 m grid. Method "near" is required because the data are
       categorical; the result is a 100 x 100 block of identical 100 m
       cells per original 10 km cell. Blocky boundaries are intentional
       and acceptable.
    3. Write INT1U + DEFLATE/PREDICTOR=2/TILED.

  Per-scenario phases and active timesteps:

  BAU (static, narrative says no expansion past 17.9%):
    protected_areas_mask_BAU.tif                 keeps  {1}      all timesteps

  NAT, SOC, CUL (time-staggered, expansion 17.9% -> ~30% by 2030):
    protected_areas_mask_<X>_phase0_current.tif  keeps  {1}      2024, 2028
    protected_areas_mask_<X>_phase1_target.tif   keeps  {1,2}    2032
    protected_areas_mask_<X>_phase2_full.tif     keeps  {1,2,3}  2036-2060

  Cell counts (out of ~158 M land cells at 100 m):
    BAU                      28,330,871   ~17.9%
    NAT phase0 / SOC ph0 / CUL ph0 same                ~17.9% (identical
                                                        across scenarios:
                                                        same Current PAs)
    NAT phase1               44,597,597   ~28%
    SOC phase1               43,568,510   ~28%
    CUL phase1               44,580,520   ~28%
    NAT phase2               48,688,963   ~30%
    SOC phase2               48,683,953   ~30%
    CUL phase2               48,671,725   ~30%

  Narrative choice for BAU (Option A in the design discussion):
    The Deléglise BaU output proposes some expansion to 25%, but our BAU
    scenario narrative (scenario_narratives.txt L134) says "there is no
    expansion of conservation areas beyond the existing coverage of 17.88%".
    We trust our narrative — only value=1 cells are kept for the BAU mask.
    Values 2 and 3 in Final_results_BaU.tif are deliberately ignored.

USED BY
  NAT, SOC, CUL: Conservation_expansion_and_preservation
                 (Mask_type: Dynamic, with per-timestep file references —
                  see the YAML snippet printed at the end of the build log)
  BAU:           Conservation_existing_only and ineffective_conservation
                 (Mask_type: Static, single file)


===============================================================================
4. indigenous_lands_mask.tif                                             [DONE]
===============================================================================

WHAT IT IS
  Binary mask of titled Indigenous community lands in Peru, combining the
  two legally distinct Peruvian categories: comunidades nativas
  (Amazonian) and comunidades campesinas (Andean/coastal). Inside the
  mask, a fixed set of high-impact transitions are suppressed by NAT and
  CUL's Indigenous_land_OECM intervention:
  forested_areas -> high_intensity_agricultural_areas / mining /
  built_up_and_barren_lands, and low_intensity_agricultural_areas ->
  high_intensity_agricultural_areas. Other transitions pass through
  unchanged.

  Two source datasets, mostly disjoint geographically, unioned via
  per-cell OR:

  Source A — SICNA (Amazonian comunidades nativas tituladas)
    spatial_masks/comunidad-nativa-titulada/
      comunidad-nativa-titulada.shp + sidecar files.
      1,911 polygons, all category_i = 8, key = "comunidad-nativa-titulada".
      CRS: SIRGAS 2000 (GCS_SIRGAS_2000).
      DBF last updated 2023-01-27.

    Source: IBC (Instituto del Bien Común) — SICNA database
            (Sistema de Información sobre Comunidades Nativas de la
            Amazonía Peruana, founded 1996, maintained by IBC since 1998).
    Download channel: LandMark Global Platform (landmarkmap.org), which
            republishes IBC SICNA data for Peru. Identified by the
            platform-style schema (feature_id, category_i, key slug,
            name_strin / name_es with [Departamento][País] bracket
            formatting, parent_id).
    Coverage: 920 features in Loreto, 352 in Ucayali, 198 in Amazonas,
            182 in Junín, 98 in Pasco, 87 in Cusco (Selva Central),
            etc. Amazon-skewed by design — SICNA covers only comunidades
            nativas (Amazonian Indigenous peoples). Andean comunidades
            campesinas are SICCAM's domain (Source B below).

  Source B — Comunidades campesinas tituladas (Andean / coastal)
    spatial_masks/comunidades-campesinas-tituladas-cofopri/
      comunidades-campesinas-tituladas.shp + sidecar files.
      4,177 polygons. CRS: WGS 84.
      DBF last updated 2020-07-14.

    Source: MINAGRI redistribution of multiple government title
            registries:
              "TRANSFERENCIA DE COFOPRI DS-018-2014-VIVIENDA" — 3,309 polygons
              GORE (regional governments) — 466 polygons
              SUNARP (legal title registry) — 275 polygons
              SBN (Superintendencia Nacional de Bienes Estatales) — 125 polygons
    Download channel: geogpsperu.com redistribution of the MINAGRI
            consolidated file (filename "Comunidades Campesinas Minagri
            geogpsperu juansuyo.shp", direct Google Drive download).
    Coverage: 813 in Ancash, 773 in Cusco, 538 in Puno, 349 in Lima, 268
            in Ayacucho, 171 in Huánuco, 166 in La Libertad, 155 in
            Piura, 146 in Apurímac, 142 in Junín, 124 in Huancavelica,
            107 in Arequipa, etc. Andean / coastal-skewed by design.

    Note: This is NOT the canonical SICCAM dataset (which has ~7,267
    registered communities, ~5,100 with mapped polygons per IBC's most
    recent reporting). It's the closest freely-downloadable proxy,
    pulled from COFOPRI government cadastral transfers. The SICCAM
    superset can be requested from IBC via LandMark
    (landmarkmap.org/data-methods/access-data) or directly at
    ibcperu.org/servicios/siccam. Both require a registration form;
    a future upgrade.

PROCESSING
  build_indigenous_lands_mask.R does:
    1. Load both shapefiles via terra::vect().
    2. Reproject each to the project grid CRS (WGS 84) — SICNA from
       SIRGAS 2000, campesinas already in WGS 84.
    3. Rasterize each independently onto the project grid (template:
       urban_settlement_mask.tif), terra::rasterize(field = 1L,
       background = NA, touches = TRUE).
    4. Per-cell OR union via terra::ifel(!is.na(A) | !is.na(B), 1L, NA).
    5. Write INT1U + DEFLATE/PREDICTOR=2/TILED.

  Result, per source:
    SICNA-only mask cells:         21,831,136   ~13.5% of Peru
    COFOPRI-only mask cells:       20,297,253   ~12.6% of Peru
    Overlap (in both sources):         10,939   ~0.007% (transition zones
                                                  like Selva Central in
                                                  Pasco/Junín)
    Combined mask cells:           42,117,450   ~26.0% of Peru

  The overlap is tiny because the two datasets are nearly disjoint —
  SICNA covers Amazonian comunidades nativas, COFOPRI covers Andean
  comunidades campesinas. The union approximately doubles coverage vs
  SICNA alone.

  Caveats:
    - Static snapshot. CUL narrative anticipates further titling through
      2060; for MVP we use the snapshot.
    - The COFOPRI source is older (2020 vs SICNA's 2023) and probably
      under-counts newer titles. Upgrading to SICCAM via LandMark would
      bring this up to date and likely add ~25% more communities.
    - Suppression magnitudes inside the mask are "first-instinct" values
      pending validation with project partners (see protocol §3.7).

USED BY
  NAT, CUL: Indigenous_land_OECM


===============================================================================
5. low_es_value_mask.tif                                                 [DONE]
===============================================================================

WHAT IT IS
  Binary mask defining "low ecosystem-service value" areas where new mining
  concessions are permitted under SOC's Mining_in_low_ES_areas. A cell is in
  the mask if its NCP_baseline value falls in the lowest quartile (Q25) of
  the NCP_baseline distribution WITHIN its NASCENT modelling region — so
  "low" is judged comparatively per region, not against a national absolute
  threshold.

INPUT
  spatial_masks/NCP_baseline.tif
    24,192 x 16,579, WGS 84, ~100 m, FLT4S.
    Values 0..0.686 (normalized NCP / "Nature's Contributions to People"
    index for Peru, 2022). Higher = more ES value. 163,153,714 non-NA cells.
    Source: NASCENT project's own InVEST-based NCP modelling pipeline.
    For the Mining_in_low_ES_areas intervention we only need the 2022
    baseline (no future projections), so this single file is sufficient.

  regions/regions.tif  (in the project's regions/ folder, not in
  spatial_masks/)
    72,574 x 49,736, WGS 84, ~30 m, INT2U.
    Region legend (4 NASCENT modelling regions):
      0  =  Andes
      1  =  Coast
      2  =  Andean jungle
      3  =  Amazon basin
      55537 = NA / outside any region.
    Resolution is 3x finer than the project grid; the build downsamples by
    nearest-neighbour before use.

PROCESSING
  build_low_es_value_mask.R does:
    1. Load NCP_baseline (100 m, project grid).
    2. Load regions raster (30 m), resample to 100 m by nearest neighbour to
       match NCP_baseline. Treat 55537 (and any values outside 0..3) as NA.
    3. Mask NCP to "in any of the 4 regions" so per-region quantiles are
       computed only over valid cells.
    4. terra::zonal() with fun = quantile(probs = 0.25) -> one Q25 value
       per region.
    5. Build a "per-cell Q25 threshold" raster by classify()-mapping each
       region's integer to its Q25 value.
    6. Final mask: 1 where NCP_baseline <= per-cell Q25 threshold,
       NA elsewhere. Cells outside the regions raster get NA.
    7. Write INT1U + DEFLATE/PREDICTOR=2/TILED.

  Per-region Q25 of NCP_baseline (snapshot at last build):
    Region 0 (Andes):           0.262
    Region 1 (Coast):           0.100   <- arid desert pulls the threshold
                                            down to ~0.10
    Region 2 (Andean jungle):   0.377
    Region 3 (Amazon basin):    0.361

  Result: 40,777,549 mask cells = exactly 25.0% of region-covered area;
  ~323,000 km² ~25.1% of Peru's land area.

  Caveats:
    - Regions raster may not fully cover all Peruvian land pixels. Cells
      outside any region get NA in the mask, which means "no intervention"
      there — acceptable default.
    - "Low ES value" is per-region. A cell with NCP = 0.30 may be IN the
      mask in Region 1 (low) but NOT in Region 2 (above its Q25 of 0.377).
      This matches the SOC narrative ("new mining limited to areas with
      comparatively lower values for ES and biodiversity") which is
      implicitly relative.

USED BY
  SOC: Mining_in_low_ES_areas


===============================================================================
WHAT LIVES HERE vs IN spatial_masks_misc/
===============================================================================

This folder contains ONLY what the spatial-interventions pipeline reads at
simulation time:

  - The five (sets of) binary intervention masks (#1-5 above).
  - This README.txt.
  - urban_settlement_mask_methodology.txt (full methodology document for #1).

Everything used to BUILD the masks — source vectors and rasters, build
scripts, run logs, terra temp dirs — lives in the sibling folder
../spatial_masks_misc/. That separation keeps this directory minimal and
makes the gitignore rules simpler. Specifically, spatial_masks_misc/ holds:

  - NCP_baseline.tif   (source for low_es_value_mask.tif; from the
                        NASCENT project's InVEST-based NCP pipeline)
  - Results_NASCENT_priorities_protected_areas_deleglise/
                       (source for protected_areas_mask_*.tif; Deléglise
                        et al. 2024 outputs + report PDF + weights Excel)
  - comunidad-nativa-titulada/
                       (source vector for indigenous_lands_mask.tif —
                        SICNA via LandMark)
  - comunidades-campesinas-tituladas-cofopri/
                       (source vector for indigenous_lands_mask.tif —
                        COFOPRI / MINAGRI via geogpsperu)
  - build_urban_settlement_mask.R             Script for #1.
  - build_urban_settlement_mask.log
  - build_urban_settlement_mask_threshold_sweep.R    Threshold-sweep utility
                                                     used during parameter
                                                     selection (#1).
  - build_urban_settlement_mask_threshold_sweep.log
  - build_mining_concessions_mask.R          Script for #2.
  - build_mining_concessions_mask.log
  - build_protected_areas_masks.R            Script for #3.
  - build_protected_areas_masks.log
  - build_indigenous_lands_mask.R            Script for #4.
  - build_indigenous_lands_mask.log
  - build_low_es_value_mask.R                Script for #5.
  - build_low_es_value_mask.log
  - .tmp_urban_mask/, .tmp_low_es/           Tempdirs for terra during the
                                              build runs. Empty between runs.

  Note: the build scripts reference input/output paths that were valid when
  the masks were originally produced (e.g. "spatial_masks/<output>.tif").
  If you re-run any of them after this reorganisation, update the
  output_path in the script (and the ref_grid path if it pointed at
  another mask in this folder) accordingly.


===============================================================================
MASKS NOT YET IN THIS FOLDER (planned)
===============================================================================

  None. All planned spatial intervention masks are now produced.

  Note on Mountain_urban_decline (BAU): the originally-planned
  mountain_mask.tif was dropped on 2026-06-05. The intended within-region
  "rural-to-urban migration" pattern under BAU in the Andes is captured
  by (a) regional demand differences (Andes BAU has smaller urban-growth
  demand than the Coast) and (b) the fitted transition models, which
  predict urban concentration in existing Andean centres (Cusco, Huaraz,
  Cajamarca, etc.). A spatial intervention is not needed; the original
  "high-elevation = decline" framing would have wrongly suppressed growth
  in exactly the high-elevation cities that ARE attracting in-region
  migrants. See protocol §10 changelog for full rationale.


For full project context (scenario definitions, intervention semantics,
YAML schema, integration with allocation.r), see
../spatial_interventions_integration_protocol.md.
