.PHONY: up down schema seed api test lint

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

api:
	uvicorn metrics_service.main:app --reload --host 0.0.0.0 --port 8000

test:
	pytest -v --cov=metrics_service

lint:
	ruff check . && ruff format --check .
