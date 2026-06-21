SHELL := /bin/bash

COMPOSE_SERVER_FILE := docker-compose-server.yaml
COMPOSE_PROJECT_NAME_SERVER := tp_grupal_server

compose-server = COMPOSE_PROJECT_NAME=$(COMPOSE_PROJECT_NAME_SERVER) docker compose -f $(COMPOSE_SERVER_FILE)

SPECS := $(patsubst %.yaml,%,$(notdir $(wildcard scenarios/specs/*.yaml)))
SPEC ?= default

# --- Validación de resultados -----------------------------------------------
DATASET     ?= small
CLIENT_NAME ?= 0
CLIENT_COMPOSE_FILE = /tmp/tp-client-$(DATASET)-$(CLIENT_NAME).yaml
CLIENT_PROJECT_NAME = tp_grupal_client_$(DATASET)_$(CLIENT_NAME)

# Muestra el exit code de cada contenedor de un proyecto dado.
# Uso: $(call show_exit_codes,<project_name>)
# 137 = SIGKILL (graceful shutdown roto).
define show_exit_codes
	echo "--- Exit codes ---" && \
	docker ps -a --filter "label=com.docker.compose.project=$(1)" \
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

# ---------------------------------------------------------------------------

help:
	@echo ""
	@echo "Uso: make <target> [FLAGS]"
	@echo ""
	@echo "--- Servidor ---"
	@echo "  up-server [SPEC=default]                              Genera y levanta el servidor (foreground, Ctrl-C graceful stop)."
	@echo "  down-server                                           Para el servidor y elimina contenedores + volúmenes."
	@echo "  logs                                                  Muestra los logs del servidor."
	@echo ""
	@echo "--- Clientes ---"
	@echo "  up-client SPEC=<s> [DATASET=small] [CLIENT_NAME=N]   Levanta un cliente contra el servidor en ejecución."
	@echo "  down-client [CLIENT_NAME=N]                          Para y elimina el cliente N."
	@echo "  down-all-clients                                      Para y elimina todos los clientes activos."
	@echo ""
	@echo "--- Todo junto ---"
	@echo "  down                                                  Para servidor y todos los clientes."
	@echo ""
	@echo "--- Validación de resultados ---"
	@echo "  generate-ref [DATASET=small]     Calcula la referencia serial para el dataset (correr una vez)."
	@echo "  compare [DATASET=small]          Compara results/<DATASET>/client_<CLIENT_NAME>/ contra la referencia."
	@echo "          [CLIENT_NAME=0]"
	@echo ""
	@echo "--- Tests ---"
	@echo "  test                             Corre go test ./... en src/."
	@echo ""
	@echo "  Specs disponibles: $(SPECS)"
	@echo ""
.PHONY: help

# --- Servidor ---------------------------------------------------------------

up-server:
	@cd src && go run ./tools/compose-gen ../scenarios/specs/$(SPEC).yaml server > ../$(COMPOSE_SERVER_FILE)
	@trap '$(compose-server) stop -t 30 && $(call show_exit_codes,$(COMPOSE_PROJECT_NAME_SERVER))' INT TERM EXIT; \
	COMPOSE_HTTP_TIMEOUT=300 COMPOSE_PROJECT_NAME=$(COMPOSE_PROJECT_NAME_SERVER) docker compose -f $(COMPOSE_SERVER_FILE) up --build --remove-orphans
.PHONY: up-server

down-server:
	@$(compose-server) stop -t 30
	@$(call show_exit_codes,$(COMPOSE_PROJECT_NAME_SERVER))
	@$(compose-server) down -v
.PHONY: down-server

logs:
	@$(compose-server) logs --follow
.PHONY: logs

# Filtra solo eventos de muerte/revival de sentinels para depuración de tolerancia a fallos.
sentinel-logs:
	@$(compose-server) logs -f --timestamps sentinel_0 sentinel_1 sentinel_2 chaos_monkey \
		| grep --line-buffered -iE \
		  "killed|peer restarted|peer is back|Sentinel started|proclaiming|new leader|starting election|peer cooldown"
.PHONY: sentinel-logs

# Monitorea liveness de sentinels en tiempo real.
# Imprime una línea cada vez que cambia el número de vivos.
# Imprime [CRITICAL] si en algún momento los 3 están caídos.
watch-sentinels:
	@echo "[$$(date -u +%T)] Monitoring sentinel liveness..." && \
	zero_events=0; prev_alive=-1; \
	count_alive() { \
		alive=0; \
		for s in sentinel_0 sentinel_1 sentinel_2; do \
			running=$$(docker inspect --format='{{.State.Running}}' "$$s" 2>/dev/null); \
			[ "$$running" = "true" ] && alive=$$((alive+1)); \
		done; \
		echo $$alive; \
	}; \
	while true; do \
		alive=$$(count_alive); \
		if [ "$$alive" -ne "$$prev_alive" ]; then \
			if [ "$$alive" -eq 0 ]; then \
				sleep 0.1; \
				alive2=$$(count_alive); \
				if [ "$$alive2" -eq 0 ]; then \
					zero_events=$$((zero_events+1)); \
					echo "[$$(date -u +%T)] [CRITICAL] 0/3 sentinels alive! (occurrence #$$zero_events)"; \
					prev_alive=0; \
				fi; \
			else \
				echo "[$$(date -u +%T)] $$alive/3 sentinels alive"; \
				prev_alive=$$alive; \
			fi; \
		fi; \
		sleep 0.2; \
	done
.PHONY: watch-sentinels

# --- Clientes ---------------------------------------------------------------

up-client:
	@cd src && go run ./tools/compose-gen ../scenarios/specs/$(SPEC).yaml client $(CLIENT_NAME) $(DATASET) > $(CLIENT_COMPOSE_FILE)
	@trap 'COMPOSE_PROJECT_NAME=$(CLIENT_PROJECT_NAME) docker compose --project-directory $(CURDIR) -f $(CLIENT_COMPOSE_FILE) stop -t 30 && $(call show_exit_codes,$(CLIENT_PROJECT_NAME))' INT TERM EXIT; \
	COMPOSE_HTTP_TIMEOUT=300 COMPOSE_PROJECT_NAME=$(CLIENT_PROJECT_NAME) docker compose --project-directory $(CURDIR) -f $(CLIENT_COMPOSE_FILE) up --build --remove-orphans
.PHONY: up-client

down-client:
	@COMPOSE_PROJECT_NAME=$(CLIENT_PROJECT_NAME) docker compose --project-directory $(CURDIR) -f $(CLIENT_COMPOSE_FILE) stop -t 30
	@$(call show_exit_codes,$(CLIENT_PROJECT_NAME))
	@COMPOSE_PROJECT_NAME=$(CLIENT_PROJECT_NAME) docker compose --project-directory $(CURDIR) -f $(CLIENT_COMPOSE_FILE) down
	@rm -f $(CLIENT_COMPOSE_FILE)
.PHONY: down-client

down-all-clients:
	@for f in /tmp/tp-client-*.yaml; do \
		[ -f "$$f" ] || continue; \
		suffix=$$(basename "$$f" .yaml | sed 's/tp-client-//'); \
		project=tp_grupal_client_$$(echo "$$suffix" | tr '-' '_'); \
		echo "Stopping client $$suffix..."; \
		COMPOSE_PROJECT_NAME=$$project docker compose --project-directory $(CURDIR) -f "$$f" stop -t 30 2>/dev/null || true; \
		COMPOSE_PROJECT_NAME=$$project docker compose --project-directory $(CURDIR) -f "$$f" down 2>/dev/null || true; \
		rm -f "$$f"; \
	done
.PHONY: down-all-clients

down:
	@$(MAKE) --no-print-directory down-server
	@$(MAKE) --no-print-directory down-all-clients
.PHONY: down

# --- Validación de resultados -----------------------------------------------

generate-ref:
	@python3 compare_results.py generate --dataset $(DATASET)
.PHONY: generate-ref

compare:
	@python3 compare_results.py compare --dataset $(DATASET) --results-dir results/$(DATASET)/client_$(CLIENT_NAME)
.PHONY: compare

# --- Tests ------------------------------------------------------------------

test:
	@cd src && go test ./...
.PHONY: test
