SHELL := /bin/bash
VENV  := .venv

# Host exports ROS2 py3.12 paths on PYTHONPATH; they leak into the py3.14 venv.
PY      := env -u PYTHONPATH $(VENV)/bin/python
PIP     := $(PY) -m pip
COMPOSE := docker compose
TEST_IMAGE := mini-data-platform/airflow-test:local

HADOLINT   := docker run --rm -i hadolint/hadolint hadolint
SHELLCHECK := docker run --rm -v "$(PWD):/mnt" -w /mnt koalaman/shellcheck:stable

.DEFAULT_GOAL := help

.PHONY: help
help: ## Show targets
	@grep -hE '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) \
		| awk 'BEGIN{FS=":.*?## "}{printf "  \033[36m%-14s\033[0m %s\n", $$1, $$2}'

.PHONY: venv
venv: ## Create venv, install pinned deps
	env -u PYTHONPATH /usr/bin/python3.14 -m venv $(VENV)
	$(PIP) install -q --upgrade pip
	$(PIP) install -q -r requirements-dev.txt

.PHONY: secrets
secrets: ## Write random creds into .env
	./scripts/gen_secrets.sh

.PHONY: doctor
doctor: ## Preflight checks
	./scripts/doctor.sh

.PHONY: lint
lint: ## ruff, yamllint, hadolint, shellcheck
	$(PY) -m ruff check .
	$(PY) -m ruff format --check .
	$(PY) -m yamllint -s .
	$(HADOLINT) - < docker/airflow/Dockerfile
	$(SHELLCHECK) scripts/*.sh config/postgres/*.sh

.PHONY: test
test: ## Unit tests, no Docker
	$(PY) -m pytest tests/unit -q

.PHONY: dags
dags: ## Fail on any DAG import error (runs inside the Airflow image)
	docker build -q --target test -t $(TEST_IMAGE) -f docker/airflow/Dockerfile . >/dev/null
	docker run --rm --entrypoint python $(TEST_IMAGE) \
		-m pytest /opt/airflow/tests/test_dag_integrity.py -q

.PHONY: validate
validate: ## Check compose resolves
	$(COMPOSE) config -q && echo "compose OK"

# --wait rejects any exited container, so only long-running services are listed.
# The one-shots (minio-init, airflow-init) are pulled in as declared
# dependencies and waited on via service_completed_successfully.
DAEMONS := postgres minio airflow-apiserver airflow-scheduler airflow-dag-processor

.PHONY: up
up: ## Start stack, wait for healthy
	$(COMPOSE) up -d --wait --wait-timeout 300 $(DAEMONS)

.PHONY: down
down: ## Stop stack, keep volumes
	$(COMPOSE) down

.PHONY: nuke
nuke: ## Stop stack, delete volumes
	$(COMPOSE) down -v

.PHONY: ps
ps: ## Service status
	$(COMPOSE) ps

.PHONY: logs
logs: ## Tail logs
	$(COMPOSE) logs -f --tail=100

.PHONY: seed
seed: ## Generate a batch, upload to MinIO
	$(PY) -m data_generator.generate --upload

.PHONY: e2e
e2e: ## MinIO -> Airflow -> Postgres -> Metabase, incl. idempotency
	$(PY) -m pytest tests/integration -q

.PHONY: ci
ci: lint validate test dags ## CI fast tier

.PHONY: ci-full
ci-full: ci up e2e ## Fast tier + integration

.PHONY: dump-logs
dump-logs: ## Collect logs for CI artifacts
	mkdir -p out/logs
	$(COMPOSE) logs --no-color > out/logs/compose.log 2>&1 || true
