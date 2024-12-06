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
    python3.11 -m venv .venv
    source .venv/bin/activate # On Windows: .venv\Scripts\activate
    ```

3. Install the package with development dependencies:

    ```bash
    pip install -e ".[dev]"
    ```

4. Set up infrastructure configuration:

    ```bash
    # Copy and edit Terraform variables
    cp terraform/environments/development/terraform.tfvars.template terraform/environments/development/terraform.tfvars
    
    # Edit terraform.tfvars with your values:
    # - project_id
    # - service_account
    # - oura_api_token
    # etc.
    ```

## Infrastructure Deployment

1. Initialize Terraform:

    ```bash
    cd terraform/environments/development
    terraform init
    ```

2. Deploy infrastructure and generate environment configuration:

    ```bash
    ./scripts/deploy.sh
    ```

    This script will:
    - Apply Terraform configuration
    - Generate `.env` file from Terraform outputs
    - Deploy DAGs to Cloud Composer
    - Set up required resources

3. Verify deployment:

    ```bash
    # Check if .env file was generated
    cat .config/development/.env
    
    # Verify infrastructure
    terraform output
    ```

## Configuration Management

This project uses a two-tier configuration approach:

1. **Infrastructure Configuration** (`terraform.tfvars`):
   - Contains all infrastructure-related variables
   - Source of truth for resource names and settings
   - Used by Terraform to provision infrastructure

2. **Application Configuration** (`.env`):
   - Automatically generated from Terraform outputs
   - Used by Python applications and scripts
   - Contains derived values from infrastructure

### Managing Configurations

- **Adding New Variables**:
  1. Add to `variables.tf`
  2. Set value in `terraform.tfvars`
  3. Add to `outputs.tf` if needed by applications
  4. Update `generate_env.sh` if needed

- **Updating Existing Values**:
  1. Modify `terraform.tfvars`
  2. Run `./scripts/deploy.sh` to apply changes
  3. New `.env` file will be generated automatically

### Security Notes

- Never commit `terraform.tfvars` or `.env` to version control
- Keep sensitive values in Secret Manager (future enhancement)
- Use service account keys only for local development

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
        storage.googleapis.com \
        composer.googleapis.com \
        compute.googleapis.com \
        cloudresourcemanager.googleapis.com \
        iam.googleapis.com
    ```

3. Create service account and download key:

    ```bash
    # Create service account
    gcloud iam service-accounts create kirill-markin-mac \
        --display-name="Kirill Markin Mac Service Account"

    # Download service account key
    gcloud iam service-accounts keys create .config/development/service-account.json \
        --iam-account=kirill-markin-mac@stefans-body-etl.iam.gserviceaccount.com
    ```

4. Set up environment variables:

    ```bash
    # Create .env file in .config/development/
    mkdir -p .config/development
    cp .config/development/.env.template .config/development/.env

    # Edit .env file with your values
    # GOOGLE_APPLICATION_CREDENTIALS=.config/development/service-account.json
    # OURA_API_TOKEN=your_token_here
    ```

## Running the Pipeline

1. Configure Oura Ring API token:
   - Generate token at `https://cloud.ouraring.com/personal-access-tokens`
   - Add to `.config/development/.env` as `OURA_API_TOKEN`

2. Run the pipeline:

    [ ] TODO: Add script to run the pipeline

3. If you need to reset the pipeline:

    ```bash
    # Drop existing BigQuery tables
    ./scripts/drop_tables.sh
    ```

## Monitoring

- View raw data in GCS: `gs://oura-raw-data/raw/oura/`
- Query data in BigQuery: `stefans-body-etl.oura_data.*`
- Check logs in Cloud Logging for detailed pipeline execution information

## Monitoring & Management URLs

### Main Dashboards

- Project Dashboard: `https://console.cloud.google.com/home/dashboard`
- APIs Dashboard: `https://console.cloud.google.com/apis/dashboard`
- Enabled APIs: `https://console.cloud.google.com/apis/enabled`

### Data Storage & Processing

- BigQuery Dataset: `https://console.cloud.google.com/bigquery`
- Cloud Storage Browser: `https://console.cloud.google.com/storage/browser`
- Cloud Composer (Airflow): `https://console.cloud.google.com/composer/environments`

### Monitoring & Logging

- Cloud Monitoring: `https://console.cloud.google.com/monitoring`
- Cloud Logging: `https://console.cloud.google.com/logs/query`
- Error Reporting: `https://console.cloud.google.com/errors`

### Security & IAM

- IAM & Admin: `https://console.cloud.google.com/iam-admin/iam`
- Service Accounts: `https://console.cloud.google.com/iam-admin/serviceaccounts`

### Deployment & Infrastructure

- Cloud Console: `https://console.cloud.google.com/home/dashboard`
- Cloud Storage Browser: `https://console.cloud.google.com/storage/browser`
- BigQuery: `https://console.cloud.google.com/bigquery`
- Cloud Composer: `https://console.cloud.google.com/composer/environments`

Note: Append `?project=YOUR_PROJECT_ID` to any URL to view resources for a specific project.

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

### Deployment

```bash
./scripts/deploy.sh
```

### Contributing

Please read [CONTRIBUTING.md](docs/contributing.md) for details on our code of conduct and the process for submitting pull requests.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
