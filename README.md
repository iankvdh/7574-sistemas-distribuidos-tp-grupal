# TP Sistemas Distribuidos — Pipeline por strategy

## Layout

```
scenarios/
  1.yaml, 2_serial.yaml     # composes legacy (hand-written)
  default.yaml              # compose generado desde specs/default.yaml
  4_scaled.yaml             # compose generado desde specs/4_scaled.yaml
  specs/
    default.yaml            # spec declarativo (fuente de verdad)
    4_scaled.yaml
scripts/switch.sh           # picker interactivo
src/                        # código Go (client, gateway, worker, common, tools)
results/                    # output local (montado por drains)
```

Cada `scenarios/specs/*.yaml` es la fuente declarativa; `compose-gen` lo expande a `scenarios/*.yaml` (un docker-compose listo para `up`).

## Makefile

| Target                              | Qué hace |
|-------------------------------------|----------|
| `make gen [SPEC=name]`              | Regenera `scenarios/<name>.yaml` desde `scenarios/specs/<name>.yaml`. Default `SPEC=default`. |
| `make gen-all`                      | Regenera todos los `scenarios/*.yaml` que tengan spec. |
| `make switch`                       | Picker interactivo: lista escenarios, regenera si tiene spec, copia el elegido a `docker-compose.yaml`. |
| `make up`                           | `docker compose up --build -d` y sigue logs del `docker-compose.yaml` actual. |
| `make up-pipeline [SPEC=name]`      | Atajo: `gen` + copy + `up`. |
| `make up-legacy`                    | Atajo para el escenario legacy (`scenarios/2_serial.yaml`). |
| `make down`                         | Para y borra todos los containers + network. |
| `make logs`                         | `docker compose logs` del compose actual. |
| `make test`                         | `go test ./...` desde `src/`. |

## Generador de docker-compose

El binario `src/tools/compose-gen` toma un spec declarativo y emite un docker-compose. Es **agnóstico al tipo de strategy**: solo conoce env vars (`STRATEGY`, `INPUT`, `OUTPUT_i`, etc.). Para sumar un comportamiento nuevo basta con registrarlo en el worker y agregar un bloque al spec.

### Forma del spec (`scenarios/specs/<name>.yaml`)

```yaml
clients: 1
gateways: 1
transactions: /datasets/small_trans.csv   # opcional, default /datasets/LI-Small_Trans.csv
accounts: /datasets/small_accounts.csv    # opcional, default /datasets/LI-Small_accounts.csv

workers:
  - name: <prefijo>            # service name (suffix _0,_1,... si replicas>1)
    strategy: <STRATEGY>       # nombre registrado en src/worker/strategy/
    replicas: 2                # ring queues se auto-derivan si >1
    input: queue:NAME          # o direct_exchange:NAME[:KEY] — el input siempre es 1 cola/exchange
    match_count: 1             # cuántos outputs son "match" (filters); omitido en joiners/drains
    outputs:                   # ordenados: primeros match_count son match, resto nomatch
      - direct_exchange:foo
      - queue:bar
      - sharded_queues:prefix:K  # K queues nombradas prefix_0..prefix_{K-1};
                                 # runtime rutea cada mensaje a la queue elegida
                                 # por hashing.Shard(client_id, K)
    env:                       # extras propios de la strategy
      EXPECTED_EOFS: "3"
    volumes:                   # opcional (drains usan esto para escribir a /results)
      - ./results:/results
```

### Cómo agregar un escenario nuevo

1. Crear `scenarios/specs/<nombre>.yaml` con la topología deseada.
2. `make gen SPEC=<nombre>` → genera `scenarios/<nombre>.yaml`.
3. `make up-pipeline SPEC=<nombre>` o `make switch` para levantarlo.

### Cómo agregar un tipo de strategy nuevo (reducer, aggregator, ...)

1. Crear `src/worker/strategy/<tipo>/<algo>.go` que implemente la interfaz `strategy.Strategy` y se registre con `strategy.Register(...)` desde `init()`.
2. Importar el paquete via blank import en `src/worker/main.go`.
3. Referenciarlo desde cualquier spec por nombre. Si necesita env vars propias, declararlas bajo `env:`.

El generador no tiene que conocer el tipo nuevo: solo serializa lo que está en el spec.

## Strategies disponibles

| Strategy                              | Tipo      | Resumen |
|---------------------------------------|-----------|---------|
| `filter_period1`                      | filter    | Match: transactions dentro de `[2022-09-01, 2022-09-05]`. |
| `filter_period2`                      | filter    | Match: transactions dentro de `[2022-09-06, 2022-09-15]`. |
| `filter_wire_ach`                     | filter    | Match: `payment_format ∈ {Wire, ACH}`. |
| `filter_currency_usd_p1` / `_p2` / `_other_periods` | filter | Match: `payment_currency == "US Dollar"`. |
| `filter_amount_lt_50`                 | filter    | Match: `amount_paid < 50.0`. |
| `joiner_usd`                          | joiner    | Forwardea cada tx; espera `EXPECTED_EOFS` EOFs por cliente y emite uno unificado. |
| `drain`                               | sink      | Escribe cada tx + EOF marker a `DRAIN_OUTPUT_FILE`. Para validación end-to-end. |
| `noop`                                | passthrough | Reenvía todo a cada output. |

## Sharding por client_id

Cuando un consumer-type escala a `K>1` y necesita que todos los datos +
EOFs de un cliente caigan en la misma réplica (caso del `joiner_usd`),
se usa `sharded_queues:PREFIX:K` en el output del upstream. El generador
abre K queues nombradas (`PREFIX_0..PREFIX_{K-1}`) y, al publicar, el
runtime calcula `hashing.Shard(client_id, K)` (FNV-1a) para elegir la
queue destino. Cada réplica `i` consume de `queue:PREFIX_i` (el spec lo
declara así explícitamente). La función de hash es la misma en todos los
productores, así que la afinidad por cliente queda garantizada sin
necesidad de extender el middleware AMQP (mismo patrón que el TP2).

## Coordinación de EOF

Cada filter con `N>1` réplicas hace un anillo de **1 ronda**: solo el iniciador (la réplica que recibió el EOF del upstream) emite los EOFs agregados a cada output. Esto garantiza que cada cola downstream reciba **exactamente 1 EOF por cliente**, sin importar cuántas réplicas haya upstream. Si los counts no convergen, el iniciador re-encola el EOF en su input y otra réplica reinicia el anillo.

`joiner_usd` usa otra topología: espera `EXPECTED_EOFS` EOFs por cliente (uno por rama upstream) y emite uno solo con el total agregado.

## Flujo típico

```bash
make gen-all              # regenera composes desde todos los specs
make switch               # elegí el escenario; copia a docker-compose.yaml
make up                   # levanta y sigue logs
# (en otra terminal) inspeccionar results/drain_*.csv
make down                 # tira todo abajo
```
