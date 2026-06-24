# TP Grupal - Money Laundering Analysis

Sistema distribuido para detección de anomalías en transacciones bancarias.
Implementa cinco consultas analíticas sobre un dataset de transferencias: transacciones menores a 50 USD, máximos por banco, anomalías contra el promedio por formato de pago, patrón scatter-gather, y micro-transacciones con formato de pago Wire/ACH convertidas a USD.

## Integrantes

| Nombre y Apellido | Padrón | Email |
|-------------------|--------|-------|
| Juan Martín de la Cruz | 109588 | <jdelacruz@fi.uba.ar> |
| Ian Klaus von der Heyde | 107638 | <ivon@fi.uba.ar> |
| Agustín Altamirano | 110237 | <aaltamirano@fi.uba.ar> |

---

## Pre-requisitos

- Docker y Docker Compose v2
- Go 1.22+ (solo para modificar o compilar localmente)
- Python 3 (solo para `make compare` / `make generate-ref`)
- Datasets en formato CSV de transacciones y cuentas en `datasets/`

---

## Flujo de uso

El sistema separa el servidor (gateways + workers + RabbitMQ + sentinels) de los clientes. El servidor se levanta una vez y los clientes se conectan de forma independiente, cada uno en su propia terminal.

### 1. Levantar el servidor

```bash
make up-server SPEC=chaos
```

Donde `SPEC` es el nombre de uno de los escenarios definidos en la carpeta `/scenarios/specs`. El servidor queda corriendo en primer plano. Presionando Ctrl-C se detiene de forma _graceful_.

### 2. Levantar clientes

En terminales separadas, una por cliente:

```bash
make up-client SPEC=chaos DATASET=small CLIENT_NAME=0
make up-client SPEC=chaos DATASET=medium CLIENT_NAME=1
make up-client SPEC=chaos DATASET=large CLIENT_NAME=2
```

Donde `DATASET` determina qué CSVs lee el cliente (`small` / `medium` / `large`, default `small`).
`CLIENT_NAME` es un identificador libre (número o string) que nombra el subdirectorio donde se guardan los resultados: `results/<dataset>/client_<CLIENT_NAME>/`.

`SPEC` debe coincidir con el valor especificado para el servidor (ver posibles valores en la sección [Specs disponibles](#specs-disponibles)). `up-client` usa el spec para saber cuántos gateways hay y cómo se llaman, y genera un compose de un único cliente en `/tmp/tp-client-<CLIENT_NAME>.yaml`. Ese archivo se borra automáticamente al hacer `down-client` o `down`.

Cada cliente es completamente independiente. Se puede correr cualquier combinación de datasets y clientes contra el mismo servidor.

### 3. Bajar el sistema

```bash
make down-server                  # para el servidor
make down-client CLIENT_NAME=0    # para un cliente específico
make down-all-clients             # para todos los clientes activos
make down                         # para servidor y todos los clientes (borra volúmenes con sus datos persistidos)
```

### 4. Generar la referencia serial

```bash
make generate-ref DATASET=small
make generate-ref DATASET=medium
make generate-ref DATASET=large
```

Calcula los resultados correctos de forma serial y los guarda en `notebooks/<dataset>/reference/`. Solo es necesario correrlo una vez.

### 5. Comparar resultados

```bash
make compare DATASET=small CLIENT_NAME=0
make compare DATASET=medium CLIENT_NAME=1
```

`CLIENT_NAME` debe ser el mismo identificador usado al levantar ese cliente. Compara todos los CSVs de `results/<dataset>/client_<CLIENT_NAME>/` contra la referencia y reporta OK/FAIL por query.

---

## Specs disponibles

Los specs definen la topología del servidor: número de gateways y réplicas por worker. El dataset se pasa por parámetro al momento de correr.

| Spec | Gateways | Sentinels | Réplicas | Uso recomendado |
|------|----------|-----------|----------|-----------------|
| `minimal` | 1 | 3 | 1 por worker | modelo mínimo |
| `default` | 2 | 3 | 3 en workers de alta carga | Dataset small/medium, múltiples clientes |
| `scaled` | 4 | 5 | Ajustadas según uso de CPU | Dataset más grades, alta concurrencia |
| `chaos` | 4 | 3 | = `scaled` | Demo de tolerancia a fallos: el Chaos Monkey mata workers, gateways y sentinels al azar. |
| `chaos_no_gateway_kill` | 4 | 3 | = `scaled` | Chaos Monkey mata workers y sentinels, pero no gateways ni fetchers. |
| `chaos_no_gateway_no_sentinel_kill` | 4 | 3 | = `scaled` | Chaos Monkey mata solo workers (no gateways, sentinels ni fetchers). |
| `chaos_sentinel_only` | 4 | 3 | = `scaled` | Chaos Monkey mata **solo** sentinels. Prueba el self-revival entre pares. |

Los specs `chaos*` agregan dos bloques sobre la topología base: `sentinels` + `sentinels_env` (los procesos de monitoreo que reviven nodos caídos) y `chaos_monkey` (el inyector de fallas). Ver más abajo cómo se leen.

---

## Referencia de targets de Makefile

| Target | Descripción |
|--------|-------------|
| `make up-server SPEC=<s>` | Genera el compose del servidor y lo levanta en primer plano. |
| `make up-client SPEC=<s> [DATASET=small] [CLIENT_NAME=N]` | Levanta un cliente contra el servidor en ejecución. |
| `make down-server` | Para y elimina los contenedores del servidor. |
| `make down-client [CLIENT_NAME=N]` | Para y elimina el cliente N. |
| `make down-all-clients` | Para y elimina todos los clientes activos. |
| `make down` | Para y elimina servidor y todos los clientes (borra volúmenes con sus checkpoints). |
| `make generate-ref [DATASET=small]` | Calcula los CSVs de referencia (correr una vez por dataset). |
| `make compare [DATASET=small] [CLIENT_NAME=N]` | Compara `results/<dataset>/client_<N>/` contra la referencia. |
| `make logs` | Muestra logs del compose de servidor actual. |
| `make watch-sentinels` | Monitorea cuántos sentinels están vivos; imprime una línea cada vez que cambia el número y un `[CRITICAL]` si en algún momento llegan a 0/N. |
| `make help` | Lista todos los targets y los specs disponibles. |

---

## Cómo leer y modificar los specs

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
| `queue:NAME` | Cola dedicada de una réplica. |
| `bound_queue:QUEUE:EXCHANGE:{REPLICA_ID}` | Igual que la anterior, pero bindeada a un exchange por routing key. |
| `direct_exchange:NAME:{REPLICA_ID}` | Exchange directo; cada réplica escucha su propia routing key. |
| `sharded_queues:PREFIX:K` | K colas `PREFIX_0..PREFIX_{K-1}`; el runtime rutea por `hash(client_id) mod K`. |
| `batch_queues:PREFIX:K` | K colas `PREFIX_0..PREFIX_{K-1}`; rutea el **batch entero** a un shard elegido por `hash(client_id, seq_id)`, de modo que cada origin id lo procesa una sola réplica por etapa. |
| `final_queues` | Solo en `final_joiner`; se expande a una cola por gateway. |

> **Nota sobre `batch_queues` vs `sharded_queues`:** ambos shardean, pero `sharded_queues` agrupa por `client_id` (afinidad para nodos stateful) y `batch_queues` distribuye carga por batch sin subdividir sus ítems.

### Sentinels y Chaos Monkey (specs `chaos*`)

```yaml
sentinels: 3                         # cantidad de procesos sentinel
sentinels_env:
  SENTINEL_HB_INTERVAL_SECONDS: 1    # cada cuánto un sentinel emite heartbeat a sus pares
  SENTINEL_PEER_TIMEOUT_SECONDS: 3   # sin heartbeat por este tiempo ⇒ par considerado caído
  SENTINEL_PEER_GRACE_SECONDS: 5     # ventana de gracia antes de actuar sobre un par
  SENTINEL_PEER_COOLDOWN_SECONDS: 12 # tras revivir un par, no volver a tocarlo por este tiempo
  SENTINEL_DETECTION_INTERVAL_SECONDS: 1
  OK_TIMEOUT_SECONDS: 3              # timeout del health-check a un worker monitoreado

chaos_monkey:
  exclude:                           # nombres de servicio que el monkey NUNCA mata
    - fetcher_q5
  env:
    KILL_INTERVAL_SECONDS: 20        # cada cuánto mata un container al azar
    KILL_TIMEOUT_SECONDS: 10
```

Los sentinels se monitorean entre sí (heartbeats por UDP + control por TCP, elección de líder por **bully**) y monitorean a los workers; al detectar una caída reinician el container vía docker-from-docker. `TARGETS` (qué containers puede matar el monkey) y `EXCLUDE` los calcula `compose-gen` automáticamente a partir del spec, no se deben editar a mano. `exclude` permite sacar nodos puntuales del conjunto de nodos a monitorear.

### Fórmula de invarianza de los sentinels

Si se desea modificar los parámetros de configuración de tiempos de los sentinels, se tiene que tener en cuenta los siguientes invariantes. El tiempo que tarda un sentinel caído en volver a estar operativo es:

```txt
revival_time = SENTINEL_PEER_TIMEOUT + OK_TIMEOUT + T_restart_docker
```

Para que el cluster sea indestructible, `SENTINEL_PEER_COOLDOWN` debe cumplir:

```txt
revival_time  <  SENTINEL_PEER_COOLDOWN  <  KILL_INTERVAL × N_sentinels
```

- **Cota inferior** (`revival_time < COOLDOWN`): evita restart loops (no reiniciar un nodo que todavía está levantando).
- **Cota superior** (`COOLDOWN < KILL_INTERVAL × N`): garantiza que un sentinel bloqueado por cooldown salga de él **antes** de que el monkey alcance a matar las N réplicas. Si `COOLDOWN ≥ KILL_INTERVAL × N`, el mismo nodo puede seguir bloqueado mientras el monkey elimina las restantes, y por lo tanto se produce un  colapso total (0/N vivos).

Solo hay que declarar los parámetros de lógica de negocio (`SUSPICIOUS_THRESHOLD`, `MIN_INTERMEDIATES`, `AMOUNT_THRESHOLD_PCT`, `BUFFER_DIR`, `MAX_CONVERTED_AMOUNT_USD`). Los de coordinación entre workers los infiere el generador.

### Agregar un spec nuevo

1. Crear `scenarios/specs/<nombre>.yaml`.
2. `make up-server SPEC=<nombre>` para verificar que expande sin errores y levantarlo.