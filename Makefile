.PHONY: help up down logs build migrate proto test test-unit test-integration lint format typecheck clean

help:
	@echo "eventsphere — common commands:"
	@echo "  make up               Start the full stack (docker compose up --build)"
	@echo "  make down             Stop the stack and remove volumes"
	@echo "  make logs             Tail logs from every service"
	@echo "  make migrate          Run Alembic migrations against the running stack"
	@echo "  make proto            Regenerate gRPC stubs from proto/inventory.proto"
	@echo "  make test             Run the full test suite (needs local Postgres+Redis; see README)"
	@echo "  make lint             Run ruff + mypy"
	@echo "  make format           Auto-format with ruff"

up:
	docker compose -f infra/docker-compose.yml up --build

down:
	docker compose -f infra/docker-compose.yml down -v

logs:
	docker compose -f infra/docker-compose.yml logs -f

migrate:
	docker compose -f infra/docker-compose.yml run --rm migrate

proto:
	./scripts/generate_proto.sh

test:
	pytest -v --cov=services --cov-report=term-missing

test-unit:
	pytest -v -m "not integration and not contract" tests/unit

test-integration:
	pytest -v -m integration tests/integration

lint:
	ruff check .
	mypy services --config-file pyproject.toml

format:
	ruff format .
	ruff check --fix .

clean:
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	rm -rf .pytest_cache .mypy_cache .ruff_cache .coverage htmlcov
