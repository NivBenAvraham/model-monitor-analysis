#!/bin/bash
# Refresh AWS CodeArtifact credentials.
# Must be run once every 4 hours before accessing Athena or S3.
#
# Usage:
#   source scripts/refresh_aws_credentials.sh
#
# Note: use `source` (not `bash`) so the exports apply to your current shell session.

export AWS_ARTIFACT_TOKEN=$(aws codeartifact get-authorization-token \
  --region us-east-1 \
  --domain beehero \
  --query authorizationToken \
  --output text)

export UV_INDEX_BEEHERO_PYPI_USERNAME=aws
export UV_INDEX_BEEHERO_PYPI_PASSWORD="$AWS_ARTIFACT_TOKEN"

echo "AWS CodeArtifact credentials refreshed. Valid for 4 hours."
