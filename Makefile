# Makefile for Ceres

.PHONY: help build test fmt clippy clean docker-up docker-down migrate

help: ## Show this help message
	@echo 'Usage: make [target]'
	@echo ''
	@echo 'Available targets:'
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "  %-15s %s\n", $$1, $$2}' $(MAKEFILE_LIST)

build: ## Build the project
	cargo build

release: ## Build the project in release mode
	cargo build --release

test: ## Run tests
	cargo test

fmt: ## Format code
	cargo fmt

fmt-check: ## Check code formatting
	cargo fmt --check

clippy: ## Run clippy lints
	cargo clippy --all-targets --all-features -- -D warnings

clean: ## Clean build artifacts
	cargo clean

docker-up: ## Start PostgreSQL with docker-compose
	docker-compose up -d

docker-down: ## Stop PostgreSQL
	docker-compose down

migrate: ## Run database migrations
	@echo "Initializing migration tracking..."
	@psql $$DATABASE_URL -v ON_ERROR_STOP=1 -c "CREATE TABLE IF NOT EXISTS schema_migrations (filename text PRIMARY KEY, applied_at timestamptz NOT NULL DEFAULT now());" > /dev/null
	@echo "Checking for pending migrations..."
	@for f in migrations/*.sql; do \
		[ -f "$$f" ] || continue; \
		filename=$$(basename "$$f"); \
		escaped_filename=$$(printf '%s' "$$filename" | sed "s/'/''/g"); \
		applied=$$(psql $$DATABASE_URL -t -A -c "SELECT 1 FROM schema_migrations WHERE filename = '$$escaped_filename'" 2>/dev/null || echo "0"); \
		if [ "$$applied" = "1" ]; then \
			echo "  ✓ $$filename (already applied)"; \
		else \
			echo "  → Running $$filename..."; \
			if psql $$DATABASE_URL -v ON_ERROR_STOP=1 -f "$$f"; then \
				psql $$DATABASE_URL -v ON_ERROR_STOP=1 -c "INSERT INTO schema_migrations (filename) VALUES ('$$escaped_filename');" > /dev/null; \
				echo "  ✓ $$filename (applied successfully)"; \
			else \
				echo "  ✗ $$filename (failed)"; \
				exit 1; \
			fi; \
		fi; \
	done
	@echo "Migration complete!"

dev: docker-up ## Start development environment
	@echo "PostgreSQL started. Run 'make migrate' to initialize the database."

all: fmt clippy test ## Run fmt, clippy, and tests
