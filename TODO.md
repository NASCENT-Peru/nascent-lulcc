# TODOs for the project

- At the moment the trans_rates_df contain a transition from Mining despite the being penalized this is not contained ni the allocation params. Need to either remove the transition from mining or add it to the allocation params.

- There are quite a number of transitions in the Andes region that do not have corresponding models fitted. Check whether feature selection is removing all features and thus not fitting a model, or whether there are just no transitions in the training data for these transitions.

- Have claude or codex go over the transition modelling to try and fix the large model errors and improve the robustness of objects to missing parameters values e.g. the error with ranger importance.mode I encountered in allocation.R

-   Prepare some basic interventions use the methods of the \*\_interventions.yml file.

-   For running the project locallly use rv for environment and package management.

-   Allocation.R

    -   Download and setup Dinamica 8 as per Jan's instructions.

    -   re-route dinamica log files

    -   use consistent log_msg function within parallel loop

    -   run profiling over the inner function preparing probability maps for a given time step to test whether regional parallelisation is the most efficient vs. parallelisation over the transition predictions.

    - In run_allocation_one_timestep, region_rast and current_lulc need to be read in inside the future loop because they are non-exportable objects.