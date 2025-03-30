# CMS Open Payments Data Ingestion with Airflow

This project uses Apache Airflow running in Docker to download, process, and ingest CMS (Centers for Medicare & Medicaid Services) Open Payments data into Google Cloud Platform (GCP), specifically Google Cloud Storage (GCS) and BigQuery.

## Overview

The project contains two main Airflow DAGs:

<<<<<<< HEAD
1.  **`GCP_ingestion_CMS_RSRCH`**: Ingests the **Research (RSRCH)** payment data from CMS for the years 2018 through 2023.
=======
1.  **`GCP_ingestion_CMS_RSRCH`**: Ingests the **Research (RSRCH)** payment data from CMS for the years 2017 through 2023.
>>>>>>> 6465183d7ce7c893cf5c5076539a1eb364af646d
2.  **`GCP_ingestion_CMS`**: Ingests **Research (RSRCH)**, **Ownership (OWNRSHP)**, and **General (GNRL)** payment data from CMS for the years 2017 through 2019.

Both DAGs perform the following high-level steps for each specified year:
*   Download the relevant yearly data zip file from `download.cms.gov`.
*   Extract the CSV files from the zip archive.
*   Convert the target CSV file(s) (identified by keywords like `DTL_RSRCH`, `DTL_OWNRSHP`, `DTL_GNRL`) into Parquet format.
*   Upload the Parquet file(s) to a specified GCS bucket (`raw/` directory).
*   Load the data from GCS into BigQuery tables using external tables and merge operations.
*   Clean up local temporary files.

## Prerequisites

*   **Docker:** [Install Docker](https://docs.docker.com/get-docker/)
*   **Docker Compose:** Usually included with Docker Desktop. [Install Docker Compose](https://docs.docker.com/compose/install/) if needed.
*   **GCP Account:** A Google Cloud Platform account with billing enabled.
*   **GCP Project:** A GCP project where the resources (GCS bucket, BigQuery dataset) will reside.
*   **GCP Service Account:** A service account with permissions to write to GCS and create/manage BigQuery tables and jobs (e.g., roles like `Storage Object Admin`, `BigQuery Data Editor`, `BigQuery Job User`). Download the service account key file (JSON).

## Setup

1.  **GCP Resources:**
    *   Ensure you have created the necessary GCS bucket and BigQuery dataset in your GCP project. The DAGs are currently configured to use:
        *   GCP Project ID: `dtc-de-course-447715`
        *   GCS Bucket: `cms_bucket_jj`
        *   BigQuery Dataset: `CMS`
    *   You may need to update these values directly in the DAG Python files (`dags/CMS_rsrch.py` and `dags/CMS.py`) if you use different names or projects.

2.  **GCP Credentials:**
    *   Place your downloaded GCP service account key file (JSON) into the `google/` directory within this project folder (e.g., `D:/Git/CMS_Data_Engineer/airflow/google/your-service-account-key.json`).
    *   **Important:** The `docker-compose.yaml` file mounts the local `google` directory to `/opt/airflow/google` inside the Airflow containers.

3.  **Airflow GCP Connection:**
    *   Once Airflow is running (see next step), navigate to the Airflow UI.
    *   Go to `Admin` -> `Connections`.
    *   Create a new connection or edit the existing `google_cloud_default` connection (if it exists).
    *   Set the `Conn Id` to `gcp-airflow` (this is the ID used in the DAGs).
    *   Set the `Conn Type` to `Google Cloud`.
    *   In the `Keyfile Path` field, enter the path *inside the container* where the service account key is located, e.g., `/opt/airflow/google/your-service-account-key.json`.
    *   Specify the `Project Id` if it's not inferred from the key file.
    *   Save the connection.

## Running the Project

1.  Navigate to this project directory (`D:/Git/CMS_Data_Engineer/airflow`) in your terminal.
2.  Build and start the Airflow services in detached mode:
    ```bash
    docker-compose up -d --build
    ```
    This command will build the Docker image (installing dependencies from `requirements.txt`) and start the Postgres database, Airflow scheduler, and Airflow webserver containers. The initialization service will run first to set up the database and default users.

## Accessing Airflow UI

*   Open your web browser and go to: `http://localhost:8080`
*   Log in using the default credentials:
    *   Username: `admin` / Password: `admin`
    *   OR Username: `airflow` / Password: `airflow`
*   You should see the `GCP_ingestion_CMS_RSRCH` and `GCP_ingestion_CMS` DAGs listed. By default, they are paused. Unpause them to start the scheduled runs or trigger them manually.

## Dependencies

Python dependencies required for the Airflow workers are listed in `requirements.txt` and are automatically installed during the Docker image build process.
