SHELL := /bin/bash
COMPOSE_FILE := docker-compose.yaml
COMPOSE_PROJECT_NAME := tp_grupal

compose = COMPOSE_PROJECT_NAME=$(COMPOSE_PROJECT_NAME) docker compose -f $(COMPOSE_FILE)

# Every scenarios/specs/*.yaml is a declarative spec that compose-gen expands
# into scenarios/<name>.yaml (the compose docker actually runs).
SPECS := $(patsubst %.yaml,%,$(notdir $(wildcard scenarios/specs/*.yaml)))
SPEC ?= default

# --- Compose lifecycle -----------------------------------------------------

# Muestra el exit code de cada contenedor. 137 = SIGKILL (graceful shutdown roto).
define show_exit_codes
	echo "--- Exit codes ---" && \
	docker ps -a --filter "label=com.docker.compose.project=$(COMPOSE_PROJECT_NAME)" \
		--format '{{.Names}}' | while read name; do \
		code=$$(docker inspect --format '{{.State.ExitCode}}' "$$name" 2>/dev/null); \
		if [ "$$code" = "137" ]; then \
			echo "SIGKILL (force-killed!) $$name"; \
		elif [ "$$code" = "143" ] || [ "$$code" = "130" ]; then \
			echo "ok (signal $$code)       $$name"; \
		elif [ "$$code" = "0" ]; then \
			echo "ok (clean exit)         $$name"; \
		else \
			echo "exit($$code)            $$name"; \
		fi; \
	done && echo "-----------------"
endef

up:
	@trap '$(compose) stop -t 30 && $(show_exit_codes)' INT TERM EXIT; \
	COMPOSE_HTTP_TIMEOUT=300 COMPOSE_PROJECT_NAME=$(COMPOSE_PROJECT_NAME) docker compose -f $(COMPOSE_FILE) up --build --remove-orphans
.PHONY: up

up-detach:
	@COMPOSE_HTTP_TIMEOUT=300 COMPOSE_PROJECT_NAME=$(COMPOSE_PROJECT_NAME) docker compose -f $(COMPOSE_FILE) up --build --remove-orphans --detach
.PHONY: up-detach

down:
	@$(compose) stop -t 30
	@$(show_exit_codes)
	@$(compose) down
.PHONY: down

logs:
	@$(compose) logs
.PHONY: logs

test:
	@cd src && go test ./...
.PHONY: test

# --- Spec → compose generation --------------------------------------------

gen:
	@cd src && go run ./tools/compose-gen ../scenarios/specs/$(SPEC).yaml > ../scenarios/$(SPEC).yaml
	@echo "Wrote scenarios/$(SPEC).yaml from scenarios/specs/$(SPEC).yaml"
.PHONY: gen

gen-all:
	@for spec in $(SPECS); do \
		echo "Generating $$spec.yaml from specs/$$spec.yaml..."; \
		(cd src && go run ./tools/compose-gen ../scenarios/specs/$$spec.yaml > ../scenarios/$$spec.yaml); \
	done
.PHONY: gen-all

# --- Scenario selection ----------------------------------------------------

switch:
	@bash ./scripts/switch.sh
.PHONY: switch

# --- One-shot helpers ------------------------------------------------------

up-legacy:
	@cp ./scenarios/2_serial.yaml $(COMPOSE_FILE)
	@$(MAKE) up
.PHONY: up-legacy

up-pipeline:
	@$(MAKE) gen SPEC=$(SPEC)
	@cp ./scenarios/$(SPEC).yaml $(COMPOSE_FILE)
	@$(MAKE) up
.PHONY: up-pipeline
