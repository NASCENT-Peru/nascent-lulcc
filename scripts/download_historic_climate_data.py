# -*- coding: utf-8 -*-
# Historical-only CHELSA-CMIP6 download with multiple 30-year windows around midyears.
# - Single model
# - No SSPs (experiment_id='historical')
# - Peru extent copied from your newer code
# - Early completeness check per window/variable

import os
from chelsa_cmip6.GetClim import chelsa_cmip6

# --- Helpers ---------------------------------------------------------------

def ensure_trailing_sep(path):
    """Ensure trailing os.sep for string concatenation inside the library."""
    return path if path.endswith(os.sep) else path + os.sep

def expected_bioclim_paths_historical(output, institution_id, source_id,
                                      member_id, start_date, end_date, bioclims):
    """
    Build expected file paths for a single historical window.
    Matches the library's naming convention:
    CHELSA_{institution_id}_{source_id}_bio{token}_historical_{member_id}_{start}_{end}.nc
    Special-case 'gdd' which does not use 'bio' in the filename.
    """
    output = ensure_trailing_sep(output)
    tokens = [str(x).strip().lower().replace("bio", "") for x in bioclims]

    out = []
    for t in tokens:
        if t == "gdd":
            fname = f"CHELSA_{institution_id}_{source_id}_gdd_historical_{member_id}_{start_date}_{end_date}.nc"
        else:
            fname = f"CHELSA_{institution_id}_{source_id}_bio{t}_historical_{member_id}_{start_date}_{end_date}.nc"
        out.append(os.path.join(output, fname))
    return out

# --- Configuration ---------------------------------------------------------

# Single model (historical lives under activity_id='CMIP')
model = {'activity_id': 'CMIP',
         'institution_id': 'NOAA-GFDL',
         'source_id': 'GFDL-ESM4',
         'member_id': 'r1i1p1f1'}

# Peru bbox copied from your newer code
xmin = -81.411
xmax = -68.665
ymin = -18.348
ymax = 0.000

# Choose 30-year midyears (each becomes [mid-15 .. mid+14]).
# Adjust these as needed; keep within historical coverage (e.g., <= 2014).
midyears = [2010, 2014, 2018, 2022]  # examples -> windows: 1955–1984, 1975–2004, 1985–2014

# Build the list of historical 30-year windows (YYYY-01-15 .. YYYY-12-15)
time_periods = []
for mid in midyears:
    start_year = mid - 15
    end_year   = mid + 14
    start_date = f'{start_year}-01-15'
    end_date   = f'{end_year}-12-15'
    time_periods.append({'midyear': mid, 'start': start_date, 'end': end_date})

# Output directory
output_dir = 'Z:/NASCENT-Peru/03_data/predictors/raw/climatic/chelsa_cmip6_downloads/historic_peru/'
os.makedirs(output_dir, exist_ok=True)
output_dir = ensure_trailing_sep(output_dir)

# Bioclim variables (use your fuller set from the newer code)
bioclims = ["1","2","3","4","5","6","7","8","9","10",
            "11","12","13","14","15","16","17","18","19","gdd"]

# --- Run with early completeness check ------------------------------------

activity_id    = model['activity_id']
institution_id = model['institution_id']
source_id      = model['source_id']
member_id      = model['member_id']

for period in time_periods:
    midyear    = period['midyear']
    refps      = period['start']  # For historical, we pass the same window as ref...
    refpe      = period['end']
    fefps      = period['start']  # ...and as "fut" because the API expects both.
    fefpe      = period['end']

    # Build expected filenames for this window
    targets = expected_bioclim_paths_historical(
        output=output_dir,
        institution_id=institution_id,
        source_id=source_id,
        member_id=member_id,
        start_date=refps,
        end_date=refpe,
        bioclims=bioclims
    )

    # Determine which files are missing
    missing = [p for p in targets if not os.path.exists(p)]
    if not missing:
        print(f"Skip: alle Outputs vorhanden → {source_id} / historical / mid={midyear} ({refps}..{refpe})")
        continue

    print(f"Run: es fehlen {len(missing)} Files → {source_id} / historical / mid={midyear} ({refps}..{refpe})")
    try:
        chelsa_cmip6(
            activity_id=activity_id,
            table_id='Amon',
            experiment_id='historical',
            institution_id=institution_id,
            source_id=source_id,
            member_id=member_id,
            refps=refps,
            refpe=refpe,
            fefps=fefps,
            fefpe=fefpe,
            xmin=xmin,
            xmax=xmax,
            ymin=ymin,
            ymax=ymax,
            output=output_dir,
            use_esgf=False,
            bioclims=bioclims
        )
    except Exception as e:
        # Log and continue with the next window (do not stop whole batch)
        print(f"Fehler bei {source_id} / historical / mid={midyear}: {e}")
