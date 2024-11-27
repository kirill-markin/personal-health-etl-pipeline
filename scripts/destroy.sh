#!/bin/bash
set -e

# Load environment variables
source .config/development/.env

# Change to Terraform directory
cd terraform/environments/development

# Destroy infrastructure
terraform destroy -auto-approve

echo "Infrastructure destroyed successfully"