#!/bin/bash
set -e

cd terraform/environments/development
terraform output -raw env_file_content > ../../../.config/development/.env