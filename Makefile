SHELL := /bin/bash
VENV  := .venv

# First interpreter that satisfies pyproject's requires-python. Override with
# `make venv PYTHON=/path/to/python3.12`; do not hardcode one machine's path.
PYTHON ?= $(shell command -v python3.14 || command -v python3.13 \
                  || command -v python3.12 || command -v python3)

# Host exports ROS2 py3.12 paths on PYTHONPATH; they leak into the py3.14 venv.
PY      := env -u PYTHONPATH $(VENV)/bin/python
PIP     := $(PY) -m pip
COMPOSE := docker compose
TEST_IMAGE := mini-data-platform/airflow-test:local

# CI has these on the runner; locally they run as throwaway containers.
# Override with HADOLINT=hadolint SHELLCHECK=shellcheck.
HADOLINT   ?= docker run --rm -i hadolint/hadolint hadolint
SHELLCHECK ?= docker run --rm -v "$(PWD):/mnt" -w /mnt koalaman/shellcheck:stable

.DEFAULT_GOAL := help

.PHONY: help
help: ## Show targets
	@grep -hE '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) \
		| awk 'BEGIN{FS=":.*?## "}{printf "  \033[36m%-14s\033[0m %s\n", $$1, $$2}'

.PHONY: venv
venv: ## Create venv, install pinned deps
	@test -n "$(PYTHON)" || { echo "no python3 found on PATH"; exit 1; }
	@env -u PYTHONPATH $(PYTHON) -c "import sys; \
		sys.exit(0) if sys.version_info >= (3, 12) else \
		(print(f'need Python >= 3.12, got {sys.version.split()[0]}'), sys.exit(1))"
	env -u PYTHONPATH $(PYTHON) -m venv $(VENV)
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

.PHONY: e2e-stack
e2e-stack: up e2e ## Bring the stack up and run the end-to-end suite

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
DAEMONS := postgres minio airflow-apiserver airflow-scheduler airflow-dag-processor metabase

.PHONY: up
up: ## Start stack, wait for healthy, provision Metabase
	# --build because mini_platform is baked into the image, not bind-mounted:
	# without it a host code edit silently never reaches the DAG.
	$(COMPOSE) up -d --build --wait --wait-timeout 420 $(DAEMONS)
	$(PY) -m scripts.provision_metabase

.PHONY: provision
provision: ## Re-run Metabase provisioning (idempotent)
	$(PY) -m scripts.provision_metabase

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

.PHONY: seed-many
seed-many: ## Upload three batches at once, so ingest fans out
	@for i in 1 2 3; do \
		$(PY) -m data_generator.generate --batch-id "region_$$i" --rows 1200 \
			--seed $$((i * 11)) --corrupt 36 --duplicates 24 --upload | grep uploaded; \
	done

.PHONY: seed-bad
seed-bad: ## Upload a batch past max_reject_ratio, which the gate must refuse
	@$(PY) -m data_generator.generate --batch-id broken --rows 2000 --seed 9 \
		--corrupt 1800 --duplicates 0 --upload | grep uploaded

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
