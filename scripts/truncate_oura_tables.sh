#!/bin/bash

# List of tables to truncate
TABLES=(
    "oura_activity"
    "oura_readiness"
    "oura_sleep"
)

PROJECT="stefans-body-etl"
DATASET="oura_data"

echo "Starting to truncate tables in ${PROJECT}.${DATASET}"

for table in "${TABLES[@]}"; do
    echo "Truncating table: ${table}"
    bq query --nouse_legacy_sql "TRUNCATE TABLE \`${PROJECT}.${DATASET}.${table}\`"
    
    if [ $? -eq 0 ]; then
        echo "✓ Successfully truncated ${table}"
    else
        echo "✗ Failed to truncate ${table}"
        exit 1
    fi
done

echo "All tables truncated successfully!"