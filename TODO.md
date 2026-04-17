-   Prepare some basic interventions use the methods of the \*\_interventions.yml file.

-   For running the project locallly use rv for environment and package management.

-   Adapt Jan's logic of removing the Dinamica model entirely and directly running the allocation step: specifcally need to add a loop over regions.

    -   Download and setup Dinamica 8 as per Jan's instructions.

    -   re-route dinamica log files

    -   use consistent log_msg function within parallel loop

    -   test whether regional parallelisation is the most efficient vs. parallelisation over the transition predictions.

    -