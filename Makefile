SHELL := /bin/bash
COMPOSE_FILE := docker-compose.yaml
COMPOSE_PROJECT_NAME := tp_grupal

compose = COMPOSE_PROJECT_NAME=$(COMPOSE_PROJECT_NAME) docker compose -f $(COMPOSE_FILE)

up:
	@replicas=$$(awk -F= '/- GATEWAY_AMOUNT=/{gsub(/ /, "", $$2); print $$2; exit}' $(COMPOSE_FILE)); \
	if [ -z "$$replicas" ]; then \
		echo "No se encontró GATEWAY_AMOUNT en $(COMPOSE_FILE)"; \
		exit 1; \
	fi; \
	COMPOSE_HTTP_TIMEOUT=300 COMPOSE_PROJECT_NAME=$(COMPOSE_PROJECT_NAME) docker compose -f $(COMPOSE_FILE) up --build --remove-orphans --detach --scale gateway=$$replicas
	@$(compose) logs --follow
.PHONY: up

down:
	@$(compose) stop -t 1
	@$(compose) down
.PHONY: down

logs:
	@$(compose) logs
.PHONY: logs

test:
	@cd src && go test ./...
.PHONY: test

switch:
	@echo Escenarios de prueba:
	@echo "1) Un cliente, gateway + rabbitmq"
	@read -p "Selecciona uno [1]: " option; \
	cp ./scenarios/$${option}.yaml $(COMPOSE_FILE)
.PHONY: switch
