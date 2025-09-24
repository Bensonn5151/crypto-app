#!/bin/bash

# Airflow 3.x Setup Script
# This script will clean up old Docker containers and set up Airflow 3.x from scratch

echo "🚀 Starting Airflow 3.x setup..."

# Stop and remove all containers
echo "🛑 Stopping all Docker containers..."
docker compose down -v
docker system prune -f

# Remove old volumes (this will delete all existing data)
echo "🗑️ Removing old Docker volumes..."
docker volume rm $(docker volume ls -q --filter name=postgres) 2>/dev/null || true

# Create necessary directories
echo "📁 Creating directories..."
mkdir -p ./dags ./logs ./plugins ./config

# Set environment variable for Airflow UID
echo "🔧 Setting up environment..."
export AIRFLOW_UID=$(id -u)
echo "AIRFLOW_UID=$AIRFLOW_UID" > .env

# Initialize Airflow (this will create directories and set permissions)
echo "⚙️ Initializing Airflow..."
docker compose --profile init up airflow-init

# Initialize the database
echo "🗄️ Setting up database..."
docker compose up -d postgres redis

# Wait for postgres to be ready
echo "⏳ Waiting for PostgreSQL to be ready..."
sleep 10

# Run database migration
echo "🔄 Running database migration..."
docker compose run --rm airflow-webserver airflow db migrate

# Create admin user using the new Airflow 3.x command structure
echo "👤 Creating admin user..."
docker compose run --rm airflow-webserver airflow db init
docker compose run --rm airflow-webserver bash -c "
airflow users create \
  --username admin \
  --firstname Admin \
  --lastname User \
  --role Admin \
  --email admin@example.com \
  --password admin
"

# Start all services
echo "🚁 Starting all Airflow services..."
docker compose up -d

echo "✅ Setup complete!"
echo ""
echo "🌐 Airflow webserver will be available at: http://localhost:8080"
echo "👤 Login credentials:"
echo "   Username: admin"
echo "   Password: admin"
echo ""
echo "📊 Check status with: docker compose ps"
echo "📋 View logs with: docker compose logs -f [service_name]"
echo ""
echo "⏳ Please wait a few minutes for all services to start up completely..."