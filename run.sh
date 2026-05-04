#!/bin/bash
set -e

echo "Setting up etlt-pipeline..."

# Check dependencies
for cmd in docker python3 getent; do
  if ! command -v $cmd &>/dev/null; then
    echo "'$cmd' is required but not installed. Aborting."
    exit 1
  fi
done

# Create .env
if [ -f .env ]; then
  echo ".env already exists, skipping creation."
else
  AIRFLOW_UID=$(id -u)
  DOCKER_GID=$(getent group docker | cut -d: -f3)
  SECRET_KEY=$(python3 -c "import secrets; print(secrets.token_hex(32))")

  cat > .env <<ENVEOF
AIRFLOW_UID=$AIRFLOW_UID
DOCKER_GID=$DOCKER_GID
_PIP_ADDITIONAL_REQUIREMENTS=apache-airflow-providers-docker apache-airflow-providers-apache-spark
AIRFLOW__API__SECRET_KEY=$SECRET_KEY
ENVEOF

  echo ".env created."
fi

# Create required folders
mkdir -p airflow/logs minio-data
echo "Folders ready."

# Start services
echo "Starting Docker Compose..."
docker compose up -d

echo ""
echo "All done! Services are starting up."
echo "   Airflow UI → http://localhost:8080"
echo "   MinIO UI   → http://localhost:9001"