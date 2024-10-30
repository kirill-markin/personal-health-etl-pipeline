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
    ```

2. Install dependencies using Poetry:

    ```bash
    cd health-data-integration
    ```

3. Set up Google Cloud credentials:

    ```bash
    export GOOGLE_APPLICATION_CREDENTIALS="path/to/your/credentials.json"
    ```

## Data Sources Setup

### Oura Ring

1. Generate Personal Access Token at <https://cloud.ouraring.com/personal-access-tokens>
2. Configure token in `data_sources/oura/configs/oura_config.yaml`

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
