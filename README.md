# TP Grupal — Money Laundering Analysis

Sistema distribuido para detección de anomalías en transacciones bancarias.
Implementa cinco consultas analíticas sobre un dataset de transferencias: transacciones menores a 50 USD, máximos por banco, anomalías contra el promedio por formato de pago, patrón scatter-gather, y micro-transacciones Wire/ACH convertidas a USD.

La arquitectura es una pipeline de workers stateless/stateful comunicados por RabbitMQ. Cada tipo de worker implementa una `strategy`; la topología se declara en un spec YAML y se expande automáticamente a un `docker-compose.yaml`.

## Integrantes

| Nombre y Apellido | Padrón | Email |
|-------------------|--------|-------|
| Juan Martín de la Cruz | 109588 | jdelacruz@fi.uba.ar |
| Ian Klaus von der Heyde | 107638 | ivon@fi.uba.ar |
| Agustín Altamirano | 110237 | aaltamirano@fi.uba.ar |

---

## Pre-requisitos

- Docker y Docker Compose v2
- Go 1.22+ (solo para modificar o compilar localmente)
- Python 3 (solo para `make compare` / `make generate-ref`)
- Dataset en `datasets/` (los CSVs de transacciones y cuentas)

---

## Flujo típico

```bash
# 1. Generar los scenarios en base a los specs
make gen-all

# 2. Seleccionar un scenario:
make switch

# 3. Correrlo:
make up

# 4. Esperar que todos los clientes terminen (Ctrl-C hace graceful stop)

# 5. Generar la referencia serial para comparar contra los resultados obtenidos (solo una vez por dataset)
make generate-ref DATASET=small

# 6. Comparar resultados de ejecución serial contra la distribuída
make compare DATASET=small CLIENT=0
```

---

## Makefile

| Target | Descripción |
|--------|-------------|
| `make up-pipeline [SPEC=x]` | Genera el compose desde `specs/<x>.yaml`, lo copia a `docker-compose.yaml` y levanta el sistema. Ctrl-C hace graceful stop. |
| `make up` | Levanta el `docker-compose.yaml` actual (sin regenerar). |
| `make up-detach` | Igual que `up` pero en background. |
| `make down` | Para y elimina todos los contenedores. |
| `make logs` | Muestra logs del compose actual. |
| `make gen [SPEC=x]` | Expande `scenarios/specs/<x>.yaml` → `scenarios/<x>.yaml`. |
| `make gen-all` | Expande todos los specs de `scenarios/specs/`. |
| `make switch` | Menú interactivo para seleccionar el escenario activo. |
| `make generate-ref [DATASET=x]` | Calcula los CSVs de referencia desde el dataset (correr una vez). `DATASET` ∈ `small` \| `medium` \| `large`. |
| `make compare [DATASET=x] [CLIENT=n]` | Compara `results/<x>/client_<n>/` contra la referencia. `DATASET` ∈ `small` \| `medium` \| `large`, default `small`. |
| `make test` | Corre `go test ./...` en `src/` si los hay. |

---

## Escenarios disponibles

Los specs viven en `scenarios/specs/`. El dataset se detecta automáticamente del path de transacciones (`LI-Small_Trans.csv` → `small`, `LI-Medium_Trans.csv` → `medium`, `LI-Large_Trans.csv` → `large`) y determina el subdirectorio de resultados (`results/<dataset>/client_N/`). Para correr con otro dataset basta cambiar `transactions:` y `accounts:` en el spec.

| Spec | Clientes | Gateways | Queries | Descripción |
|------|----------|----------|---------|-------------|
| `simple_client` | 1 | 1 | Q1–Q5 | Mínimo, 1 réplica por worker. Útil para comparar contra la referencia serial. |
| `multi_client` | 2 | 2 | Q1–Q5 | 3 réplicas en filtros y joiner. |
| `multi_client_optimized` | 2 | 4 | Q1–Q5 | Réplicas ajustadas por profiling de CPU. |
| `multi_client_q1` | 2 | 2 | Q1 | Solo la rama Q1 activa. |
| `multi_client_q2` | 2 | 2 | Q2 | Solo la rama Q2 activa. |
| `multi_client_q3` | 2 | 2 | Q3 | Solo la rama Q3 activa. |
| `multi_client_q4` | 2 | 2 | Q4 | Solo la rama Q4 activa. |
| `multi_client_q5` | 2 | 2 | Q5 | Solo la rama Q5 activa. |

---

## Specs — cómo leerlos y modificarlos

Cada spec es un YAML declarativo que describe la topología. `compose-gen` lo expande a un `docker-compose.yaml` completo, inyectando automáticamente todas las variables de coordinación (N_FINAL_JOINERS, EXPECTED_EOFS, K_SUSPICIOUS_FILTERS, etc.) a partir de los replica counts.

```yaml
clients: 2
gateways: 2
log_level: info
expected_query_eofs: 5
transactions: /datasets/LI-Small_Trans.csv
accounts:    /datasets/LI-Small_accounts.csv

fetchers:
  - name: fetcher_q5
    env:
      CONVERSIONS_QUEUE_PREFIX: conversions
      CONVERSIONS_BASE_CURRENCY: USD

workers:
  - name: filter_period1
    strategy: filter_period1
    replicas: 3
    input: queue:all_transactions
    match_count: 2            # primeros 2 outputs son "match", el resto "no-match"
    outputs:
      - queue:period1_for_wire_ach
      - queue:period1_for_currency_usd_p1
      - queue:non_period1_transactions

  - name: joiner_usd
    strategy: joiner_usd
    replicas: 3
    input: queue:joiner_usd_input_{REPLICA_ID}   # {REPLICA_ID} se expande por réplica
    outputs:
      - queue:usd_for_amount_lt_50
      - queue:q2_input

  - name: final_joiner
    strategy: final_joiner
    replicas: 2
    input: direct_exchange:results:{REPLICA_ID}
    outputs:
      - final_queues    # token mágico: se expande a final_queue:final_1..final_N (N = gateways)
```

**Tipos de input/output:**

| Sintaxis | Semántica |
|----------|-----------|
| `queue:NAME` | Cola compartida (competing consumers). |
| `direct_exchange:NAME:{REPLICA_ID}` | Exchange directo; cada réplica escucha su propia routing key. |
| `sharded_queues:PREFIX:K` | K colas `PREFIX_0..PREFIX_{K-1}`; el runtime rutea por `FNV(client_id) mod K`. |
| `final_queues` | Solo en `final_joiner`; se expande a una cola por gateway. |

**Solo hay que declarar** los parámetros de lógica de negocio (`SUSPICIOUS_THRESHOLD`, `MIN_INTERMEDIATES`, `AMOUNT_THRESHOLD_PCT`, `BUFFER_DIR`, `MAX_CONVERTED_AMOUNT_USD`). Los de coordinación entre workers los infiere el generador.

### Agregar un escenario nuevo

1. Crear `scenarios/specs/<nombre>.yaml`.
2. `make gen SPEC=<nombre>` para verificar que expande sin errores.
3. `make up-pipeline SPEC=<nombre>` para levantarlo.

---

## Strategies disponibles

| Strategy | Tipo | Descripción |
|----------|------|-------------|
| `filter_period1` | filter | Transacciones en `[2022-09-01, 2022-09-05]`. |
| `filter_period2` | filter | Transacciones en `[2022-09-06, 2022-09-15]`, proyecta sin fecha. |
| `filter_wire_ach` | filter | `payment_format ∈ {Wire, ACH}`. |
| `filter_currency_usd_p1/p2/other_periods` | filter | `payment_currency == "US Dollar"`, proyecta sin moneda ni fecha. |
| `filter_amount_lt_50` | filter | `amount_paid < 50`. |
| `filter_q3` | stateful filter | Descarta transacciones P2-USD con monto ≥ 1% del promedio por formato (recibido del aggregator_q3). |
| `joiner_usd` | joiner | Espera EOFs de todas las ramas USD upstream y emite uno unificado. |
| `sharder_q1` | sharder | Distribuye resultados Q1 entre réplicas de final_joiner por client_id. |
| `sharder_q4` | sharder | Distribuye transacciones P1-USD entre réplicas de suspicious_account_filter. |
| `suspicious_account_filter` | stateful filter | Bufferiza por cuenta; emite cuentas con ≥ SUSPICIOUS_THRESHOLD destinos distintos. |
| `path_finder_q4` | stateful | Detecta pares (A→B→C) donde B es la cuenta intermedia del patrón scatter-gather. |
| `counter_q4` | aggregator | Cuenta pares distintos por cuenta; emite los que superan MIN_INTERMEDIATES. |
| `max_q2` | aggregator | Máximo de amount_paid por banco (anillo). |
| `bank_aggregator` | aggregator | Lee cuentas y agrupa nombre de banco por account_id (anillo). |
| `aggregator_q2` | joiner | Une max_q2 + bank_aggregator para emitir máximo por nombre de banco. |
| `sum_q3` | aggregator | Suma y cuenta amount_paid por payment_format para calcular promedio parcial (anillo). |
| `aggregator_q3` | aggregator | Combina parciales de sum_q3 y hace broadcast del promedio global. |
| `micro_transaction_counter` | stateful counter | Consume cotizaciones del fetcher + transacciones Wire/ACH; cuenta micro-transacciones en USD < 1. |
| `aggregator_q5` | joiner | Suma conteos parciales de micro_transaction_counter. |
| `final_joiner` | sink | Recibe resultados de todas las queries; los despacha al gateway correspondiente por client_id. |

---

## Coordinación de EOF

**Filters con anillo (N > 1 réplicas):** cuando una réplica recibe un EOF del upstream, inicia una ronda por el anillo de réplicas para acumular los conteos de cada una. Una vez completada la ronda, la réplica iniciadora emite exactamente 1 EOF por output, garantizando que cada cola downstream reciba un único EOF por cliente independientemente de cuántas réplicas haya upstream.

**Workers sin anillo** (aggregators, joiners, path_finder, counter): cada réplica es independiente y opera sobre su propio shard. El upstream ya garantiza la afinidad por client_id vía sharding FNV.

**joiner_usd:** espera `EXPECTED_EOFS` EOFs por cliente (uno por rama upstream: filter_currency_usd_p1, p2 y other_periods) y emite uno unificado.

**final_joiner:** recibe los EOFs de cada query y los despacha al gateway correspondiente usando el `gateway_id` del cliente como índice de output.

---

## Estructura del repositorio

```
scenarios/
  specs/          # fuentes declarativas (editar estos)
  *.yaml          # composes generados (no editar a mano)
src/
  client/         # cliente Go: sube CSV y recibe resultados
  gateway/        # gateway TCP ↔ RabbitMQ
  worker/         # workers con strategy pattern
    strategy/     # una carpeta por strategy
  fetcher/        # consulta cotizaciones de divisas (Q5)
  common/         # middleware: EOF, hashing, protocol, etc.
  tools/
    compose-gen/  # generador de docker-compose desde specs
datasets/         # CSVs de entrada (no versionados)
results/          # salida por dataset y cliente (no versionada)
notebooks/        # referencia serial para validación
```
