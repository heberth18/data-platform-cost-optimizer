.PHONY: help setup build up down clean logs status test

# Default target
help:
	@echo "Available commands:"
	@echo "  setup       - Initial project setup"
	@echo "  build       - Build all Docker images"
	@echo "  up          - Start all services"
	@echo "  down        - Stop all services"
	@echo "  clean       - Remove containers and volumes"
	@echo "  logs        - Show logs for all services"
	@echo "  status      - Show status of all containers"
	@echo "  test        - Run data quality tests"
	@echo "  dbt-docs    - Generate and serve dbt documentation"

# Setup project environment
setup:
	@echo "Setting up Data Platform..."
	@if command -v python3.11 >/dev/null 2>&1; then \
		echo "Using Python 3.11"; \
		python3.11 -m venv venv; \
	elif command -v python3.10 >/dev/null 2>&1; then \
		echo "Using Python 3.10"; \
		python3.10 -m venv venv; \
	else \
		echo "Using default Python"; \
		python3 -m venv venv; \
	fi
	. venv/bin/activate && pip install --upgrade pip
	. venv/bin/activate && pip install -r requirements.txt
	mkdir -p airflow/{dags,logs,plugins}
	mkdir -p scripts
	@echo "Setup complete!"

# Build Docker images
build:
	@echo "Building Docker images..."
	docker-compose build --no-cache
	@echo "Images built successfully!"

# Start all services
up:
	@echo "Starting Data Platform services..."
	docker-compose up -d
	@echo "Waiting for services to be ready..."
	sleep 30
	@echo "Platform is running!"

# Stop all services
down:
	@echo "Stopping all services..."
	docker-compose down
	@echo "Services stopped!"

# Clean everything (containers, volumes, images)
clean:
	@echo "Cleaning up everything..."
	docker-compose down -v --rmi all
	docker system prune -f
	@echo "Cleanup complete!"

# Show logs
logs:
	docker-compose logs -f

# Show container status
status:
	@echo "Container Status:"
	docker-compose ps

# Run data quality tests
test:
	@echo "Running data quality tests..."
	. venv/bin/activate && python -m pytest tests/ -v
	@echo "Tests complete!"

# Generate and serve dbt docs
dbt-docs:
	@echo "Generating dbt documentation..."
	cd dbt && dbt docs generate && dbt docs serve
	@echo "Documentation available at: http://localhost:8081"