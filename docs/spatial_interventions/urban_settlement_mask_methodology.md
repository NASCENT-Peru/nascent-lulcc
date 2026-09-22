METHODOLOGY: URBAN SETTLEMENT MASK
From GHSL built-up surface to binary intervention mask
Last updated 2026-06-05


===============================================================================
PART 1 — LOGICAL SUMMARY
===============================================================================

GOAL
We need a binary raster mask defining "settlement zones" for the
Urban_densification intervention used in the NAT, CUL and SOC scenarios. A
cell in the mask = 1 means new urban land-use is permitted to land there
under the intervention; NA means it isn't.

The mask must:
  - capture existing settlements (where people currently live);
  - include a generous growth ring around each settlement, so adjacent
    expansion is permitted;
  - drop isolated single buildings and tiny dispersed dwellings (these aren't
    "settlements" in any meaningful sense for our purposes);


APPROACH
We take the JRC Global Human Settlement Layer (GHSL) — which gives the
amount of built-up surface inside every 100 m cell — and run a morphological
clustering pipeline that is the raster equivalent of DBSCAN. Conceptually
five steps:

  1. THRESHOLD       Decide what counts as an "urban" cell. We pick cells
                     where the GHSL says at least 1500 m² of the cell's
                     10,000 m² area is built-up surface (>= 15% built-up).
                     At this density an urban cell corresponds to a
                     contiguous block of adjacent buildings (town-scale
                     fabric), not the scattered houses of an isolated
                     hamlet.

  2. CLUSTER         Group urban cells that lie within ~2 cells of each
                     other into the same "settlement". This is what makes
                     the pipeline a clustering rather than a per-cell
                     filter: a hamlet of 12 cells spaced within range of
                     each other counts as a single settlement, not 12
                     separate ones.

  3. SIZE-FILTER     Drop clusters with fewer than 10 original urban cells.
                     10 cells = 10 ha of detected built-up surface; below
                     that we're looking at single buildings, isolated
                     mines or speckle, not communities.

  4. GROWTH RING     Add a 500 m buffer around each kept settlement
                     footprint. This is the "growth frontier": cells that
                     are not currently urban but lie close enough to an
                     existing settlement that adjacent expansion is
                     plausible.

  5. WRITE MASK      Output the union of (kept settlement footprints) +
                     (growth rings) as a binary raster aligned to the
                     project's 100 m reference grid.

The final mask is the union of the existing settlement footprints (after
size filtering) and their 500 m growth rings.


===============================================================================
PART 2 — TECHNICAL SUMMARY
===============================================================================

INPUT
  GHSL R2023A "Built-up surface per 100 m grid cell", 2025 epoch (modelled).
  Source:    European Commission Joint Research Centre (JRC).
  File:      /Users/manuelkurmann/Desktop/ghs-pop/GHS_BUILT_S_2025_Peru.tif
  Shape:     24,192 rows x 16,579 cols (~400 M cells).
  CRS:       WGS 84 (EPSG:4326).
  Cell:      ~100 m at Peru's latitudes (0.000808° in degrees).
  Values:    Float, m² of built-up surface per cell (range 0 to ~7,800;
             theoretical max 10,000 = fully built).

OUTPUT
  File:      ~/Documents/ETH/PLUS/github/nascent-lulcc/spatial_masks/
             urban_settlement_mask.tif
  Shape:     24,192 x 16,579 (aligned to input).
  CRS:       WGS 84.
  Data type: INT1U.
  Values:    1 = settlement zone, NA = elsewhere.
  Encoding:  DEFLATE compressed, PREDICTOR=2, tiled.
  Size:      1.8 MB.

PARAMETERS
  threshold_m2 = 1500    # cells with >=1500 m² built-up are "urban"
  eps_m        = 250     # cells within ~2 cells merge into the same cluster
  min_cells    = 10      # clusters smaller than this are dropped
  buffer_m     = 500     # growth-frontier ring added around kept clusters

  Derived from these at the 100 m cell resolution:
    dilate_r = ceiling((eps_m/2) / 100) = 2 cells   (Step 2 dilation radius)
    buffer_r = ceiling(buffer_m / 100) = 5 cells    (Step 6 buffer radius)

  Note: integer-cell rounding means eps_m values in the range 250–400 all
  produce the same dilate_r (= 2) and therefore identical output. The
  effective merge distance between two original urban cells is determined
  jointly by the dilation reach (Chebyshev radius 2), the 200 m aggregation
  step, and the 8-connectivity used by patches() — not by eps_m alone.

PIPELINE

  STEP 1 — THRESHOLD
    urban_bin <- terra::ifel(ghsl >= threshold_m2, 1L, 0L)
    Result: 280,604 binary urban cells.

  STEP 2 — DILATE by eps/2 (= 125 m, radius 2 cells)
    Iterated 3x3 max filter via terra::focal(), 2 iterations.
    Why iterated 3x3 instead of a single 5x5 kernel: in terra, iterated 3x3
    max-focal is dramatically faster than one larger kernel and produces
    equivalent results for binary max dilation.
    Why not terra::buffer(): on this WGS 84 raster terra::buffer computes
    geodesic distances cell-by-cell, which OOM-crashes on a 32 GB machine.
    Checkpoint: checkpoint_1_dilated.tif.

  STEP 3 — CONNECTED COMPONENTS (patches)
    3a. Aggregate the dilated raster from 100 m to 200 m (factor 2,
        fun = "max"). This cuts the cell count from 400 M to 100 M for the
        labelling step, which is the memory-hot spot. At 100 m, patches()
        was OOM-killed by macOS in four earlier attempts; at 200 m it
        completes reliably.
    3b. terra::patches(directions = 8, zeroAsNA = TRUE) at 200 m labels
        each connected component with a unique integer ID.
    3c. terra::disagg(fact = 2L, method = "near") pushes cluster IDs
        back to 100 m. Each 100 m cell inherits the cluster ID of its
        containing 200 m supercell.
    3d. terra::resample(..., method = "near") to guarantee exact grid
        alignment with urban_bin if disaggregation drifted by a fractional
        cell.
    Result: 8,404 distinct connected components.

  STEP 4 — RESTRICT CLUSTER IDS TO THE ORIGINAL URBAN FOOTPRINT
    clusters_orig <- terra::ifel(urban_bin > 0, clusters, NA)
    This ensures that in Step 5, cluster size is measured in real
    urban cells (not in the dilated halo around them).
    Checkpoint: checkpoint_2_clusters_orig.tif.

  STEP 5 — FILTER CLUSTERS BY SIZE + RECLASSIFY TO BINARY
    sizes    <- terra::freq(clusters_orig)
    keep_ids <- sizes$value[sizes$count >= min_cells]
    clusters_kept <- terra::classify(clusters_orig,
                                     rcl = cbind(keep_ids, 1L),
                                     others = NA_integer_)
    classify() uses a binary-search lookup table — far faster than
    terra::subst() at this cluster count.
    Result: 1,579 clusters kept (19 % of total); 6,825 dropped (single
    buildings, sparse hamlets below the 10-ha floor).
    Urban-cell retention: 266,087 / 280,604 input urban cells (95 %).
    Checkpoint: checkpoint_3_clusters_kept.tif.

  STEP 6 — OUTWARD BUFFER (= 500 m growth frontier, radius 5 cells)
    Convert clusters_kept to 0/1 binary, then iterated 3x3 max dilation
    x 5 iterations.
    Each kept cluster's footprint is expanded outward by ~500 m.

  STEP 7 — WRITE FINAL BINARY MASK
    out <- terra::ifel(settlement > 0, 1L, NA_integer_)
    terra::writeRaster(out, output_path,
                       datatype = "INT1U", NAflag = 255,
                       gdal     = c("COMPRESS=DEFLATE", "PREDICTOR=2",
                                    "TILED=YES"))
    Final: 1,069,101 settlement-zone cells.
    Expansion factor: 3.81x the 280,604 input urban cells.

RUNTIME (full run, no resumption)
  Step 1 (read + threshold)               ~30  s
  Step 2 (dilation x 2)                  ~60  s
  Step 3 (aggregate + patches + disagg)  ~5   min
  Step 4 (restrict + checkpoint)          ~20  s
  Step 5 (freq + classify)                ~30  s
  Step 6 (buffer dilation x 5)           ~150  s
  Step 7 (write)                          ~20  s
  TOTAL                                  ~7.9 min on M2 Mac, 32 GB.

IMPLEMENTATION NOTES 
  - Checkpoint files (Steps 2, 3+4, 5) saved under .tmp_urban_mask/. Critical
    for restart after interruption — Step 3 (patches) is the long-pole step.
  - terra tempdir set under the project (NOT /tmp): macOS periodically purges
    /tmp, losing in-progress intermediates.
  - terraOptions(memfrac = 0.75) reserves ~24 GB of 32 GB system RAM for terra.
  - patches() at 100 m on this Peru-wide raster was OOM-killed in 4 attempts
    during earlier development; aggregating to 200 m before patches() and
    disaggregating after was the reliable workaround.

PARAMETER NOTES
  Characterising the chosen parameters against the actual run output.

  - Threshold (1500 m²).  Of GHSL's 5,675,545 cells with any built-up
    signal at all, 280,604 cross the >=1500 m² bar — i.e. roughly the
    top 5%. In practice this density picks out contiguous town fabric;
    a 20–30-house hamlet with scattered buildings, which typically
    produces GHSL values of 100–300 m² per cell, sits below the bar.
    That matches the intervention's purpose, which is "where new urban
    LULC may legitimately land = real urban centres + buffer".

  - Minimum cluster size (10 cells = 10 ha).  Of 8,404 connected
    components found after dilation, 1,579 (18.8%) pass the size
    filter; 6,825 (81.2%) are dropped. The dropped clusters average
    ~2.1 cells (~2 ha) — single buildings, isolated mining-camp
    footprints or pixel-level noise. Despite filtering out 81% of
    cluster candidates, only ~5% of input urban cells are lost
    (266,087 of 280,604 retained).

  - Clustering scale (eps_m = 250).  Integer-cell rounding means
    eps_m values in the range 250..400 all produce the same
    dilate_r (= 2 cells) and therefore identical output. The effective
    merge distance between two original urban cells is determined
    jointly by the dilation reach (Chebyshev radius 2), the 200 m
    aggregation step in Step 3, and the 8-connectivity used by
    patches() — not by eps_m alone.

  - Buffer (500 m, radius 5 cells).  On average each kept settlement
    occupies ~677 mask cells (~6.8 km²) after the buffer is applied.
    For a Peruvian provincial-capital-class town with ~150 ha of dense
    fabric, that yields a "settlement zone" of ~6–8 km² — a generous
    growth frontier without bleeding far into surrounding landscape.

  - Spot check across 28 named locations.  All 22 settlements tested
    fall inside the mask: 13 major cities (Lima, Trujillo, Arequipa,
    Cusco, Huaraz, Cajamarca, Puno, Tacna, etc.), 5 Amazon-frontier
    towns (Pucallpa, Iquitos, Tarapoto, Yurimaguas, Tumbes) and 4
    smaller Andean towns (Andahuaylas, Abancay, Juliaca, Ayacucho).
    All 6 wilderness checkpoints fall outside (Sechura desert,
    Atacama-coast dunes, Cordillera Blanca peaks, Nazca Lines,
    two rural points).

    Two settlements illustrate how the clustering + buffer steps
    handle edge cases gracefully:
      Juliaca's centre cell has GHSL = 1474 — just below the 1500
        threshold — but it remains in the mask because surrounding
        cells pass the threshold and absorb the centre cell into the
        cluster.
      Yurimaguas's centre cell has GHSL = 174 (well below threshold),
        but sits inside the mask via the 500 m buffer extended from
        the city's denser core a few hundred metres away.

REPRODUCIBILITY
  Script:  ~/Documents/ETH/PLUS/github/nascent-lulcc/spatial_masks/
           build_urban_settlement_mask.R
  Log:     ~/Documents/ETH/PLUS/github/nascent-lulcc/spatial_masks/
           build_urban_settlement_mask.log
