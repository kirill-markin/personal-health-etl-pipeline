#!/bin/bash
set -e

# Load environment variables
source .config/development/.env

# Initialize and apply Terraform
cd terraform/environments/development

# Initialize Terraform
terraform init

# Plan the changes
terraform plan -out=tfplan

# Apply the changes
terraform apply tfplan

# FIXME
# # Generate new .env file
# ./scripts/generate_env.sh

echo "Deployment completed successfully"
