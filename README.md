# TP Grupal — Money Laundering Analysis

Sistema distribuido para detección de anomalías en transacciones bancarias.
Implementa cinco consultas analíticas sobre un dataset de transferencias: transacciones menores a 50 USD, máximos por banco, anomalías contra el promedio por formato de pago, patrón scatter-gather, y micro-transacciones Wire/ACH convertidas a USD.

La arquitectura es una pipeline de workers stateless/stateful comunicados por RabbitMQ. Cada tipo de worker implementa una `strategy`; la topología se declara en un spec YAML y se expande automáticamente a un compose del servidor.

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

## Flujo de uso

El sistema separa el servidor (gateways + workers + RabbitMQ) de los clientes. El servidor se levanta una vez y los clientes se conectan de forma independiente, cada uno en su propia terminal.

### 1. Levantar el servidor

```bash
make up-server SPEC=default
```

El servidor queda corriendo en primer plano. Ctrl-C hace graceful stop.

### 2. Levantar clientes

En terminales separadas, una por cliente:

```bash
make up-client SPEC=default DATASET=small CLIENT_NAME=0
make up-client SPEC=default DATASET=medium CLIENT_NAME=1
make up-client SPEC=default DATASET=large CLIENT_NAME=2
```

`DATASET` determina qué CSVs lee el cliente (`small` / `medium` / `large`, default `small`).  
`CLIENT_NAME` es un identificador libre (número o string) que nombra el subdirectorio donde se guardan los resultados: `results/<dataset>/client_<CLIENT_NAME>/`.

`up-client` usa el spec para saber cuántos gateways hay y cómo se llaman, y genera un compose de un único cliente en `/tmp/tp-client-<CLIENT_NAME>.yaml`. Ese archivo se borra automáticamente al hacer `down-client` o `down`.

Cada cliente es completamente independiente. Se puede correr cualquier combinación de datasets y clientes contra el mismo servidor.

### 3. Bajar el sistema

```bash
make down-server                  # para el servidor
make down-client CLIENT_NAME=0    # para un cliente específico
make down-all-clients             # para todos los clientes activos
make down                         # para servidor y todos los clientes
```

### 4. Generar la referencia serial (una sola vez por dataset)

```bash
make generate-ref DATASET=small
make generate-ref DATASET=medium
make generate-ref DATASET=large
```

Calcula los resultados correctos de forma serial y los guarda en `notebooks/<dataset>/reference/`. Solo hay que correrlo una vez.

### 5. Comparar resultados

```bash
make compare DATASET=small CLIENT_NAME=0
make compare DATASET=medium CLIENT_NAME=1
```

`CLIENT_NAME` debe ser el mismo identificador usado al levantar ese cliente. Compara todos los CSVs de `results/<dataset>/client_<CLIENT_NAME>/` contra la referencia y reporta OK/FAIL por query.

---

## Specs disponibles

Los specs definen la topología del servidor: número de gateways y réplicas por worker. El dataset se pasa por parámetro al momento de correr.

| Spec | Gateways | Réplicas | Uso recomendado |
|------|----------|----------|-----------------|
| `minimal` | 1 | 1 por worker | modelo mínimo |
| `default` | 2 | 3 en workers de alta carga | Dataset small/medium, múltiples clientes |
| `scaled` | 4 | Ajustadas por profiling de CPU | Dataset más grades, alta concurrencia |

---

## Makefile — referencia de targets

| Target | Descripción |
|--------|-------------|
| `make up-server SPEC=<s>` | Genera el compose del servidor y lo levanta en primer plano. |
| `make up-client SPEC=<s> [DATASET=small] [CLIENT_NAME=N]` | Levanta un cliente contra el servidor en ejecución. |
| `make down-server` | Para y elimina los contenedores del servidor. |
| `make down-client [CLIENT_NAME=N]` | Para y elimina el cliente N. |
| `make down-all-clients` | Para y elimina todos los clientes activos. |
| `make down` | Para y elimina servidor y todos los clientes. |
| `make generate-ref [DATASET=small]` | Calcula los CSVs de referencia (correr una vez por dataset). |
| `make compare [DATASET=small] [CLIENT_NAME=N]` | Compara `results/<dataset>/client_<N>/` contra la referencia. |
| `make test` | Corre `go test ./...` en `src/`. |
| `make logs` | Muestra logs del compose de servidor actual. |

---

## Specs — cómo leerlos y modificarlos

Cada spec es un YAML declarativo que describe la topología del servidor. `compose-gen` lo expande inyectando automáticamente todas las variables de coordinación (N_FINAL_JOINERS, EXPECTED_EOFS, K_SUSPICIOUS_FILTERS, etc.) a partir de los replica counts.

```yaml
gateways: 2
log_level: info
expected_query_eofs: 5

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

Solo hay que declarar los parámetros de lógica de negocio (`SUSPICIOUS_THRESHOLD`, `MIN_INTERMEDIATES`, `AMOUNT_THRESHOLD_PCT`, `BUFFER_DIR`, `MAX_CONVERTED_AMOUNT_USD`). Los de coordinación entre workers los infiere el generador.

### Agregar un spec nuevo

1. Crear `scenarios/specs/<nombre>.yaml`.
2. `make up-server SPEC=<nombre>` para verificar que expande sin errores y levantarlo.

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
  specs/                   # fuentes declarativas (editar estos)
docker-compose-server.yaml # compose del servidor generado por up-server
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
