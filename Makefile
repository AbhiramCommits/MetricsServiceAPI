export PYTHONPATH := .

.PHONY: up down schema seed load load-full cdc api bench test lint

up:
	docker compose up -d
	@echo "Waiting for postgres..."
	@sleep 3

down:
	docker compose down

schema:
	python scripts/apply_schema.py

seed:
	python scripts/seed_source.py

load:
	python scripts/run_load.py

load-full:
	python scripts/run_load.py --full-refresh

cdc:
	python scripts/seed_source.py --cdc-batch

api:
	uvicorn metrics_service.main:app --reload --host 0.0.0.0 --port 8000

bench:
	python scripts/bench.py --base-url http://127.0.0.1:8000 --n 200

test:
	pytest --cov=metrics_service --cov-report=term-missing

lint:
	ruff check . && ruff format --check . && mypy metrics_service
