# Health Data Integration Platform

## Overview

This platform integrates various health and fitness data sources into a unified data warehouse using Google Cloud Platform services. It provides automated data collection, processing, and storage from multiple health tracking devices and applications.

## Supported Data Sources

- **Oura Ring (Gen 3)**
  - Sleep tracking
  - Activity monitoring
  - Readiness metrics
  - Heart rate & HRV

## Future plans

- Garmin HRM-Pro Plus
- Withings
- BlazePod
- Cronometer

## Architecture

The platform utilizes the following GCP services:

- **Storage**: Google Cloud Storage & BigQuery
- **Processing**: Google Cloud Dataflow
- **Orchestration**: Google Cloud Composer (Airflow)
- **Data Integration**: Custom Python ETL scripts
- **Metadata Management**: Google Data Catalog
- **Security & Monitoring**: IAM, Data Encryption, Cloud Monitoring & Logging

## Installation

1. Clone the repository:

    ```bash
    git clone https://github.com/yourusername/health-data-integration
    cd health-data-integration
    ```

2. Create and activate virtual environment:

    ```bash
    python -m venv venv
    source venv/bin/activate  # On Windows: venv\Scripts\activate
    ```

3. Install the package with development dependencies:

    ```bash
    # Install with development dependencies
    pip install -e ".[dev]"
    
    # Or install only runtime dependencies
    pip install -e .
    ```

4. Set up environment variables:

    ```bash
    # Create .env file in .config/development/
    mkdir -p .config/development
    touch .config/development/.env

    # Add required environment variables
    echo "OURA_API_TOKEN=your_token_here" >> .config/development/.env
    echo "GOOGLE_APPLICATION_CREDENTIALS=.config/development/service-account.json" >> .config/development/.env
    ```

## GCP Project Setup

1. Create a new GCP project or select an existing one:

    ```bash
    gcloud projects create stefans-body-etl  # Optional: if creating new project
    gcloud config set project stefans-body-etl
    ```

2. Enable required APIs:

    ```bash
    gcloud services enable \
        bigquery.googleapis.com \
        storage.googleapis.com
    ```

3. Create service account and grant permissions:

    ```bash
    # Create service account
    gcloud iam service-accounts create kirill-markin-mac \
        --display-name="Kirill Markin Mac Service Account"

    # Download service account key
    gcloud iam service-accounts keys create .config/development/service-account.json \
        --iam-account=kirill-markin-mac@stefans-body-etl.iam.gserviceaccount.com

    # Grant necessary permissions
    gcloud projects add-iam-policy-binding stefans-body-etl \
        --member="serviceAccount:kirill-markin-mac@stefans-body-etl.iam.gserviceaccount.com" \
        --role="roles/storage.objectViewer"

    gcloud projects add-iam-policy-binding stefans-body-etl \
        --member="serviceAccount:kirill-markin-mac@stefans-body-etl.iam.gserviceaccount.com" \
        --role="roles/storage.objectCreator"

    gcloud projects add-iam-policy-binding stefans-body-etl \
        --member="serviceAccount:kirill-markin-mac@stefans-body-etl.iam.gserviceaccount.com" \
        --role="roles/bigquery.dataEditor"
    ```

4. Create GCS bucket and BigQuery dataset:

    ```bash
    # Create GCS bucket
    gcloud storage buckets create gs://oura-raw-data \
        --project=stefans-body-etl \
        --location=US \
        --uniform-bucket-level-access

    # Create BigQuery dataset
    bq --location=US mk \
        --dataset \
        stefans-body-etl:oura_data
    ```

## Running the Pipeline

1. Configure Oura Ring API token:
   - Generate token at https://cloud.ouraring.com/personal-access-tokens
   - Add to `.config/development/.env` as `OURA_API_TOKEN`

2. Run the pipeline:

    ```bash
    python3 scripts/run_pipeline.py
    ```

   This will:
   - Extract data from Oura Ring API
   - Save raw data to GCS bucket
   - Transform data into structured format
   - Load data into BigQuery tables

3. If you need to reset the pipeline:

    ```bash
    # Drop existing BigQuery tables
    bq rm -f -t stefans-body-etl:oura_data.oura_sleep
    bq rm -f -t stefans-body-etl:oura_data.oura_activity
    bq rm -f -t stefans-body-etl:oura_data.oura_readiness
    ```

## Monitoring

- View raw data in GCS: `gs://oura-raw-data/raw/oura/`
- Query data in BigQuery: `stefans-body-etl.oura_data.*`
- Check logs in Cloud Logging for detailed pipeline execution information

## Data Points Collected

### Target vitals

- Blood Glucose Levels
- Blood Pressure
- Heart Rate
- HRV
- Oxygen Saturation (SpO2)
- Body Temperature

### Activity

- Activity Score / Calories burned (Oura)
- Steps per day / Walking equivalent (Oura)

### Sleep (Oura)

- Total Sleep vs Time in Bed
- Sleep Efficiency
- Resting Heart Rate
- Restfulness
- Sleep Stages (REM/Deep/Light/Awake)
- Night movements
- Night-time HRV

## Development

### Project Structure

The project follows a modular structure with separate directories for each data source and common utilities. Key directories include:

- `data_sources/` - Source-specific ETL code and configurations
- `common/` - Shared utilities and helpers
- `composer_dags/` - Airflow DAGs for data orchestration
- `deployment_manager/` - GCP infrastructure configurations

### Running Tests

```bash
./scripts/run_tests.sh
```

### Code Formatting

```bash
./scripts/format.sh
```

## Contributing

Please read [CONTRIBUTING.md](docs/contributing.md) for details on our code of conduct and the process for submitting pull requests.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
