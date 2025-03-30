# CMS Data Engineer dbt Project (dbt Cloud)

## Overview

This project contains the dbt models for the CMS Data Engineer research project, designed to be run and managed within the dbt Cloud environment. It transforms raw data into structured, analysis-ready datasets.

## Setup (dbt Cloud)

1.  **Connect Repository:** Ensure this Git repository is connected to your dbt Cloud project.
2.  **Configure Connection:** Set up the data warehouse connection (e.g., Snowflake, BigQuery) within your dbt Cloud project settings. Credentials should be managed securely within dbt Cloud.
3.  **Environment Setup:** Configure dbt Cloud environments (e.g., Development, Production) as needed. Development environments typically use personal developer credentials, while deployment environments use service accounts.

## Usage (dbt Cloud)

*   **Development:**
    *   Use the dbt Cloud IDE for developing and testing models.
    *   Run commands like `dbt run`, `dbt test`, or `dbt build` directly in the IDE command bar against your development environment.
    *   Preview results and iterate on models.
*   **Deployment (Jobs):**
    *   Production runs are typically managed via dbt Cloud Jobs.
    *   Jobs are configured to run specific dbt commands (e.g., `dbt build --vars '{'is_test_run': 'false'}'`) on a schedule or via triggers (like Git commits or API calls).
    *   Monitor job status and logs within the dbt Cloud interface.
*   **Documentation:**
    *   dbt Cloud jobs can be configured to automatically generate and host the project documentation.
    *   Access the latest documentation through the link provided in your dbt Cloud project.

## Project Structure

*   `analyses/`: Contains ad-hoc analyses, often used within the dbt Cloud IDE.
*   `macros/`: Stores custom macros used throughout the project.
*   `models/`: Contains the SQL transformation logic.
    *   `staging/`: Models in this directory perform initial cleanup, renaming, and type casting of source data (e.g., `stg_rsrch_all.sql`). It also includes schema definitions and tests (`schema.yml`).
    *   `core/`: These models represent the core business logic and transformations, building analytical tables (e.g., `fact_rsrch_all.sql`, `groupby.sql`) from the staging layer.
*   `seeds/`: Contains CSV files loaded as tables. These can be uploaded and managed via the dbt Cloud interface or kept in Git.
*   `snapshots/`: Configuration for tracking changes to mutable source data over time.
*   `dbt_project.yml`: Main dbt project configuration file. Settings here might be overridden or augmented by dbt Cloud environment settings.
*   `packages.yml`: Lists dbt package dependencies. dbt Cloud automatically runs `dbt deps` when necessary.

## Contributing

Please refer to the main project contribution guidelines. Use dbt Cloud's features for branching and pull requests if integrated with your Git provider.

