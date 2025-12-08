# -*- coding: utf-8 -*-
# Only run downloads/computation when expected outputs are missing.

import os
from chelsa_cmip6.GetClim import chelsa_cmip6

# --- Helper ---------------------------------------------------------------

def ensure_trailing_sep(path):
    """Ensure trailing os.sep for string concatenation inside the library."""
    return path if path.endswith(os.sep) else path + os.sep

def expected_bioclim_paths(output, institution_id, source_id,
                           experiment_id, member_id,
                           refps, refpe, fefps, fefpe,
                           bioclims):
    """
    Build the list of expected file paths for hist (refps..refpe)
    and futr (fefps..fefpe) periods, matching the library's naming.
    Note: The library uses `experiment_id` in filenames for both periods.
    """
    output = ensure_trailing_sep(output)
    out = []
    # normalize tokens like "bio12" -> "12", keep "gdd"
    tokens = [str(x).strip().lower().replace("bio", "") for x in bioclims]

    def _fname(var_token, start, end):
        if var_token == "gdd":
            return f"CHELSA_{institution_id}_{source_id}_gdd_{experiment_id}_{member_id}_{start}_{end}.nc"
        else:
            return f"CHELSA_{institution_id}_{source_id}_bio{var_token}_{experiment_id}_{member_id}_{start}_{end}.nc"

    # historical outputs (ref window)
    for t in tokens:
        out.append(os.path.join(output, _fname(t, refps, refpe)))

    # future outputs (fut window)
    for t in tokens:
        out.append(os.path.join(output, _fname(t, fefps, fefpe)))

    return out

# --- Configuration --------------------------------------------------------

# Models
models = [
    {'activity_id': 'ScenarioMIP', 'institution_id': 'NOAA-GFDL', 'source_id': 'GFDL-ESM4',    'member_id': 'r1i1p1f1'},
    {'activity_id': 'ScenarioMIP', 'institution_id': 'IPSL',       'source_id': 'IPSL-CM6A-LR','member_id': 'r1i1p1f1'},
    {'activity_id': 'ScenarioMIP', 'institution_id': 'MPI-M',      'source_id': 'MPI-ESM1-2-LR','member_id': 'r1i1p1f1'},
    {'activity_id': 'ScenarioMIP', 'institution_id': 'MRI',        'source_id': 'MRI-ESM2-0',  'member_id': 'r1i1p1f1'},
    {'activity_id': 'ScenarioMIP', 'institution_id': 'MOHC',       'source_id': 'UKESM1-0-LL', 'member_id': 'r1i1p1f2'},
]

# SSP scenarios
ssps = ['ssp126', 'ssp245', 'ssp370', 'ssp585']

# 5-year stepping target windows (2020..2060), 30-year windows around target_year
time_periods = []
for target_year in range(2024, 2061, 4):
    start_year = target_year - 15
    end_year   = target_year + 14
    fefps = f'{start_year}-01-15'
    fefpe = f'{end_year}-12-15'
    time_periods.append({'target_year': target_year, 'fefps': fefps, 'fefpe': fefpe})

# Peru bbox
xmin = -81.411
xmax = -68.665
ymin = -18.348
ymax = 0.000

# Reference window
refps = '1981-01-15'
refpe = '2010-12-15'

# Output directory (adjust as needed)
output_dir = './chelsa_cmip6_downloads/future/'

# Bioclim variables to compute/save
bioclims = ["1","2","3","4","5","6","7","8","9","10",
            "11","12","13","14","15","16","17","18","19","gdd"]

# Ensure base output exists
os.makedirs(output_dir, exist_ok=True)
output_dir = ensure_trailing_sep(output_dir)

# --- Run loop with early completeness check ------------------------------

for model in models:
    activity_id    = model['activity_id']
    institution_id = model['institution_id']
    source_id      = model['source_id']
    member_id      = model['member_id']

    for ssp in ssps:
        experiment_id = ssp

        for period in time_periods:
            target_year = period['target_year']
            fefps       = period['fefps']
            fefpe       = period['fefpe']

            # Build expected filenames
            targets = expected_bioclim_paths(
                output=output_dir,
                institution_id=institution_id,
                source_id=source_id,
                experiment_id=experiment_id,  # used in filenames for both periods
                member_id=member_id,
                refps=refps, refpe=refpe,
                fefps=fefps, fefpe=fefpe,
                bioclims=bioclims
            )

            # Determine which files are missing
            missing = [p for p in targets if not os.path.exists(p)]
            if not missing:
                print(f"Skip: alle Outputs vorhanden → {source_id} / {ssp} / {target_year}")
                continue  # nothing to do for this combination

            print(f"Run: es fehlen {len(missing)} Files → {source_id} / {ssp} / {target_year}")
            try:
                chelsa_cmip6(
                    activity_id=activity_id,
                    table_id='Amon',
                    experiment_id=experiment_id,
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
                # Log and continue with the next job (do not stop whole batch)
                print(f"Fehler bei {source_id} / {ssp} / {target_year}: {e}")