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
	@for f in migrations/*.sql; do \
		echo "Running $$f..."; \
		psql $$DATABASE_URL -f "$$f"; \
	done

dev: docker-up ## Start development environment
	@echo "PostgreSQL started. Run 'make migrate' to initialize the database."

all: fmt clippy test ## Run fmt, clippy, and tests
