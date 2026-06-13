# Sentinel — monitor de tolerancia a fallos (Hito 3)

El **Sentinel** detecta workers caídos **exclusivamente por ausencia de
heartbeat UDP** y los reinicia vía Docker (`docker restart`, patrón
*docker-from-docker*). Corre con **3 réplicas**; solo el **líder** toma
decisiones de restart. El líder se elige con el algoritmo **Bully** implementado
a mano, sobre un **transporte híbrido**:

- **UDP** para el ping/pong de liveness (alta frecuencia, tolerante a pérdida).
- **TCP** para los mensajes de control del Bully (Election/OK/Coord): raros pero
  críticos para la correctitud.

> Docker se usa **solo como actuador** (reiniciar). El Sentinel nunca ejecuta
> `docker ps`/`inspect`/`logs`: la decisión de *a quién* reiniciar sale 100% del
> heartbeat. (Consigna Hito 3.)

## Layout

```
sentinel/
├── main.go                 # bootstrap: config, transportes, coordinator, monitor, SIGTERM
├── Dockerfile              # alpine + docker-cli (docker-from-docker)
├── config/config.go        # parseo de env vars
├── bully/
│   ├── protocol.go         # frame [1B tipo | 1B senderID]
│   ├── ping_udp.go         # liveness ping/pong (UDP)
│   ├── control_tcp.go      # control Election/OK/Coord (TCP, conexiones efímeras)
│   └── election.go         # máquina de estados Bully
└── monitor/
    ├── monitor.go          # lastSeen + loop de detección (grace/timeout/cooldown)
    ├── heartbeat.go        # listener UDP de heartbeats de workers
    └── restarter.go        # docker restart vía exec
```

El **emisor** de heartbeat (lado worker) vive en `common/heartbeat`.

## Variables de entorno

| Variable | Default | Descripción |
|---|---|---|
| `SENTINEL_ID` | (requerido) | ID entero único 0..N-1. Gana el de mayor ID. |
| `PEERS` | (requerido) | `id:host:tcpPort:udpPort` de los **otros** sentinels, separados por coma. Ej. `1:sentinel_1:8090:8092,2:sentinel_2:8090:8092`. |
| `EXPECTED_CONTAINERS` | (requerido) | Lista completa de containers a vigilar (idéntica en las 3 réplicas). |
| `SENTINEL_BULLY_TCP_PORT` | `8090` | Puerto TCP de control Bully. |
| `SENTINEL_BULLY_UDP_PORT` | `8092` | Puerto UDP de ping/pong sentinel↔sentinel. |
| `SENTINEL_UDP_PORT` | `8091` | Puerto UDP de heartbeats de **workers**. |
| `STARTUP_GRACE_SECONDS` | `30` | Gracia inicial antes de reiniciar containers que nunca latieron. |
| `HEARTBEAT_TIMEOUT_SECONDS` | `20` | Sin heartbeat por más de esto ⇒ caído. |
| `DETECTION_INTERVAL_SECONDS` | `5` | Período del loop de detección. |
| `RESTART_COOLDOWN_SECONDS` | `60` | Mínimo entre dos restarts del mismo container. |
| `RESTART_TIMEOUT_SECONDS` | `30` | Timeout del subproceso `docker restart`. |
| `RESTART_STOP_GRACE_SECONDS` | `10` | `docker restart --time`: ventana de SIGTERM antes de SIGKILL. |
| `LEADER_PING_INTERVAL_SECONDS` | `1` | Cada cuánto el follower sondea al líder por UDP. |
| `LEADER_PING_FAILURES` | `4` | Pongs perdidos seguidos ⇒ líder caído ⇒ elección. |
| `LEADER_PONG_TIMEOUT_MS` | `800` | Espera del pong por cada ping UDP. |
| `OK_TIMEOUT_SECONDS` | `3` | Espera de `MsgOK` (TCP) tras enviar `MsgElection`. |
| `COORD_TIMEOUT_SECONDS` | `5` | Espera de `MsgCoord` (TCP) tras recibir `MsgOK`. |
| `BULLY_BOOTSTRAP_JITTER_MS` | `500` | Jitter de arranque para desincronizar elecciones iniciales. |
| `CONTROL_DIAL_TIMEOUT_MS` | `2000` | Timeout del `Dial` TCP de control. |
| `CONTROL_IO_TIMEOUT_MS` | `2000` | Timeout de lectura/escritura del control TCP. |

### Lado emisor (worker / gateway / fetcher)

| Variable | Default | Descripción |
|---|---|---|
| `CONTAINER_NAME` | (requerido para latir) | Nombre del container; va en el payload y es el que se reinicia. |
| `SENTINEL_UDP` | (requerido para latir) | Destinos UDP de las réplicas, ej. `sentinel_0:8091,sentinel_1:8091,sentinel_2:8091`. |
| `HEARTBEAT_INTERVAL_SECONDS` | `5` | Período base del latido. |
| `HEARTBEAT_JITTER_MS` | `1000` | Jitter aleatorio agregado al período. |
| `HEARTBEAT_ENABLED` | `true` | Si está en `false` (o falta `SENTINEL_UDP`), el worker no late. |

## Nota sobre `PEERS`

El plan documentaba el formato `host:tcp:udp`. La implementación incluye el
**ID explícito** (`id:host:tcp:udp`) para que el algoritmo Bully no tenga que
inferir el ID a partir del hostname. `compose-gen` genera este formato
automáticamente.
