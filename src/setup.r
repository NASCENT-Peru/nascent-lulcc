#' LULCC_CH setup: control table, config
#'
#' Creates / reads control table and config files
#'
#' @param scenario_names character vector of scenario names
#' @param step_length integer step length for simulation
#' @param data_periods character vector of data periods
#' @param regionalization logical whether to regionalize
#' @param inclusion_threshold numeric, minimum % threshold for transition inclusion.
#' This represents the number of transition instances from class X -> Y as a % of the
#' the total area of class X; a good value for this threshold is 0.5 such that if the
#' number of cells transitioning <0.5% of the total number of cells of the initial class
#' then the transition is not included. The rationale for this is that the statistical
#' model produced for the transition will be too weak due to high-imbalance
#'
#' @return list of configuration parameters
#'
#' @export

get_config <- function(
  scenario_names = c("BAU", "EI-NAT", "EI-CUL", "EI-SOC", "GR-EX"),
  step_length = 4L,
  data_periods = c("2010_2014", "2014_2018", "2018_2022"),
  regionalization = TRUE,
  inclusion_threshold = 0.5
) {
  data_basepath <- Sys.getenv(
    "EVOLAND_DATA_BASEPATH",
    unset = "/cluster/scratch/bblack/NASCENT-LULCC"
  )
  historic_lulc_basepath <- file.path(data_basepath, "lulc")
  reg_dir <- file.path(data_basepath, "regionalization")
  predictors_dir <- file.path(data_basepath, "predictors")
  predictors_prepped_dir <- file.path(predictors_dir, "prepared")
  predictors_raw_dir <- file.path(predictors_dir, "raw")
  predictors_intermediate_dir <- file.path(predictors_dir, "intermediate")
  allocation_pars_dir <- file.path(data_basepath, "allocation_parameters")
  preds_tools_dir <- file.path(predictors_dir, "tools")
  results_dir <- file.path(data_basepath, "results")
  tools_dir <- file.path(data_basepath, "tools")

  config <- list(
    data_basepath = data_basepath,
    # Base directories
    historic_lulc_basepath = historic_lulc_basepath,

    # Tools files
    LULC_aggregation_path = "lulc_schema.json", # LULC class aggregation table
    model_specs_path = file.path(tools_dir, "model_specs.yaml"), # model specifications table
    param_grid_path = file.path(tools_dir, "param-grid.xlsx"), # model hyper parameter grids
    pred_table_path = "pred_data.yaml",
    # FIXME why are the next two repeated?
    predict_param_grid_path = file.path(tools_dir, "predict_param_grid.xlsx"),
    predict_model_specs_path = file.path(tools_dir, "predict_model_specs.csv"),

    # spatial interventions table
    spat_ints_path = file.path(tools_dir, "spatial_interventions.csv"),
    viable_transitions_lists = file.path(
      tools_dir,
      "viable_transitions_lists.rds"
    ),
    model_lookup_path = file.path(tools_dir, "model_lookup.xlsx"),
    glacial_area_change_xlsx = file.path(tools_dir, "glacial_area_change.xlsx"),
    scenario_area_mods_csv = file.path(
      tools_dir,
      "simulation_lulc_areas_2060.csv"
    ),
    calibration_ctrl_tbl_path = file.path(tools_dir, "calibration_control.csv"),

    # Paths for original data files
    ref_grid_path = file.path(
      data_basepath,
      "spatial_reference_grid",
      "ref_grid.tif"
    ),
    calibration_param_dir = file.path(allocation_pars_dir, "calibration"),
    simulation_param_dir = file.path(allocation_pars_dir, "simulation"),
    trans_rate_table_dir = file.path(
      data_basepath,
      "transition_tables",
      "prepared_trans_tables"
    ),
    trans_rates_raw_dir = file.path(
      data_basepath,
      "transition_tables",
      "raw_trans_tables"
    ),
    trans_rate_extrapol_dir = file.path(
      data_basepath,
      "transition_tables",
      "extrapolations"
    ),
    best_trans_area_tables = file.path(
      data_basepath,
      "transition_tables",
      "best_area_tables.rds"
    ),
    trans_pre_pred_filter_dir = file.path(
      data_basepath,
      "transition_datasets",
      "pre_predictor_filtering"
    ),
    trans_post_pred_filter_dir = file.path(
      data_basepath,
      "transition_datasets",
      "post_predictor_filtering"
    ),

    # Configuration parameters
    ctrl_tbl_path = file.path(tools_dir, "simulation_control.csv"),
    step_length = step_length,
    # FIXME scenario_names is not used anywhere
    scenario_names = scenario_names,
    # FIXME the next two used to be read from model_specs_path;
    # we should first move to a coherent singular config
    data_periods = data_periods,
    regionalization = regionalization,
    inclusion_threshold = inclusion_threshold,

    # ancillary spatial data
    ancillary_spatial_dir = file.path(
      data_basepath,
      "ancillary_spatial_data"
    ),
    ancillary_data_table = file.path("ancillary_data.yaml"),

    # Remote data sources and local paths
    rasterized_lulc_dir = file.path(historic_lulc_basepath, "original"),
    aggregated_lulc_dir = file.path(historic_lulc_basepath, "aggregated"),
    reg_dir = reg_dir,
    bioreg_zip_remote = "https://data.geo.admin.ch/ch.bafu.biogeographische_regionen/data.zip",
    bioreg_zip_local = file.path(reg_dir, "biogeographische_regionen.zip"),

    # Predictor directories
    glacial_change_path = file.path(data_basepath, "glacial_change"),
    predictors_raw_dir = predictors_raw_dir,
    predictors_intermediate_dir = predictors_intermediate_dir,
    predictors_prepped_dir = predictors_prepped_dir,
    ch_geoms_path = file.path(predictors_raw_dir, "ch_geoms"),
    raw_pop_dir = file.path(predictors_raw_dir, "socio_economic", "population"),
    raw_employment_dir = file.path(
      predictors_raw_dir,
      "socio_economic",
      "employment"
    ),
    preds_tools_dir = preds_tools_dir,
    prepped_lyr_path = file.path(predictors_prepped_dir, "layers"),
    prepped_fte_dir = file.path(
      predictors_prepped_dir,
      "socio_economic",
      "employment"
    ),
    prepped_pred_stacks = file.path(predictors_prepped_dir, "stacks"),
    pred_db_path = file.path(
      predictors_prepped_dir,
      "predictor_database.duckdb"
    ),

    # System configuration
    reference_crs = "epsg:2056",

    # Models directories
    feature_selection_dir = file.path(
      results_dir,
      "feature_selection"
    ),
    transition_model_dir = file.path(data_basepath, "transition_models"),
    transition_model_eval_dir = file.path(
      results_dir,
      "transition_model_eval"
    ),
    prediction_models_dir = file.path(
      results_dir,
      "transition_models",
      "prediction_models"
    ),
    validation_dir = file.path(
      results_dir,
      "validation"
    ),
    spat_prob_perturb_path = file.path(
      data_basepath,
      "spat_prob_perturb"
    )
  )

  return(config)
}
