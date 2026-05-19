# Gateway + cliente + serial-analyzer (resumen de cambios)

## Resumen

Se implementó y dejó operativo el flujo:

**cliente ↔ gateway ↔ RabbitMQ ↔ serial-analyzer ↔ RabbitMQ ↔ gateway ↔ cliente**

Incluye:
- Handshake TCP binario `ConnectRequest/ConnectAck`.
- Envío de transacciones y cuentas en batches dinámicos por tamaño (`BATCH_MAX_BYTES`).
- Publicación de entrada en `all_transactions` y `all_accounts`.
- Procesamiento temporal serial (Q1..Q5) por `ClientID` en `serial-analyzer`.
- Publicación de resultados por query en `final_<gatewayID>`.
- Forwarding gateway → cliente de `QueryResult`.
- Persistencia de resultados del cliente en CSV por query.
- Escalado de gateways con `docker compose --scale gateway=N`.

---

## Alcance actual (qué sí / qué no)

| Estado | Alcance |
| --- | --- |
| Listo | Flujo completo de ingesta + resultados de prueba serial |
| Listo | Multiples gateways + selección aleatoria desde cliente |
| Listo | Retry con backoff sólo en conexión inicial |
| Listo | EOF por query (`status="EOF"`) |
| Listo | Persistencia de resultados en `results/client_X/*.csv` |
| Falta | Versión distribuida real de las 5 querys (workers por etapas) |
| Falta | Reintentos si se cae la conexión a mitad de flujo |

---

## Protocolo externo (cliente ↔ gateway)

Cada mensaje empieza con `1 byte MsgType`.

| Code | Mensaje | Payload |
| --- | --- | --- |
| `1` | `ConnectRequest` | - |
| `2` | `ConnectAck` | `short string client_id` |
| `3` | `AccountBatch` | `uint32 N` + N cuentas |
| `4` | `EndOfAccounts` | - |
| `5` | `TransactionBatch` | `uint32 N` + N transacciones |
| `6` | `EndOfTransactions` | - |
| `7` | `Ack` | - |
| `8` | `QueryResult` | `uint8 query_id` + `short string status` |

Notas:
- No existe más `EndOfResults`.
- El fin de cada query se marca con `QueryResult(query_id, "EOF")`.
- El cliente termina cuando recibió EOF de Q1..Q5.

---

## Protocolo interno (gateway/serial-analyzer ↔ RabbitMQ)

Se usa envelope JSON dentro de `Message.Body string`.

Ejemplo de mensaje de ingesta (batch):

```json
{
  "g": "tp_grupal-gateway-1",
  "c": "261ed83f-9083-470a-90b3-3bbaca3bcd26",
  "k": 1,
  "t": 0,
  "p": "...bytes..."
}
```

Ejemplo de mensaje final (resultado de query):

```json
{
  "g": "tp_grupal-gateway-1",
  "c": "261ed83f-9083-470a-90b3-3bbaca3bcd26",
  "k": 5,
  "q": 1,
  "s": "EOF"
}
```

Campos:

| Campo | Significado |
| --- | --- |
| `g` | `GatewayID` (hostname del gateway) |
| `c` | `ClientID` |
| `k` | `MsgKind` |
| `t` | total acumulado (en EOF de cuentas/transacciones) |
| `p` | payload binario serializado (batch) |
| `q` | query id (solo `FinalQueryResult`) |
| `s` | status/row textual (solo `FinalQueryResult`) |

`MsgKind`:

| Kind | Valor |
| --- | --- |
| `AllTransactionsBatch` | `1` |
| `AllTransactionsEOF` | `2` |
| `AllAccountsBatch` | `3` |
| `AllAccountsEOF` | `4` |
| `FinalQueryResult` | `5` |

Notas:
- `GatewayID` es obligatorio: evita que un gateway consuma resultados de clientes conectados a otro.
- `FinalEOF` ya no se usa.

---

## Flujo implementado

1. Cliente arma lista de gateways: `GATEWAY_PREFIX + i + ":" + GATEWAY_PORT`.
2. Elige uno al azar y prueba conexión con retry exponencial + jitter.
3. Handshake: `ConnectRequest -> ConnectAck(client_id)`.
4. Envía `TransactionBatch*` y luego `EndOfTransactions` (ACK por batch y por EOF).
5. Envía `AccountBatch*` y luego `EndOfAccounts` (ACK por batch y por EOF).
6. Gateway publica envelopes en `all_transactions` / `all_accounts`.
7. `serial-analyzer` consume, procesa Q1..Q5 por sesión `(gatewayID, clientID)`.
8. `serial-analyzer` publica resultados en `final_<gatewayID>`.
9. Gateway consume su cola final y reenvía `QueryResult` al socket del cliente.
10. Cliente persiste filas y cierra al recibir EOF de las 5 querys.

---

## Persistencia de resultados (cliente)

- Directorio configurable por `RESULTS_DIR`.
- Un archivo por query y por cliente: `<client_id>_q<query>.csv`.
- Se escribe header + filas.
- Cuando llega EOF de una query, se cierra ese archivo.

Headers actuales:

| Query | Header CSV |
| --- | --- |
| Q1 | `From Bank,Account,To Bank,Account.1,Amount Paid` |
| Q2 | `From Bank,Account,Bank Name,Amount Paid` |
| Q3 | `From Bank,Account,Payment Format,Amount Paid` |
| Q4 | `Bank,Account` |
| Q5 | `count` |

---

## Manejo de cierre y robustez

| Punto | Implementación |
| --- | --- |
| Señales | `SIGINT`/`SIGTERM` en cliente, gateway y serial-analyzer |
| Gateway shutdown | `sync.Once`, cierre listener, cierre sockets, stop consumer final, cierre middlewares |
| Serial-analyzer shutdown | `sync.Once`, stop consumers de `all_*`, cierre de colas finales abiertas, cierre middlewares |
| Concurrencia de sockets | `ClientRegistry` + `WriteWithLock` por conexión de cliente |
| ACK/NACK en colas | ACK en mensajes válidos o descartados; NACK en error recuperable al publicar resultados |
| Límite de payload textual | `status` de `QueryResult` limitado a 255 bytes (short string) |

---

## Cambios por módulo

| Tipo | Archivos | Cambio principal |
| --- | --- | --- |
| Nuevo | `src/gateway/config/config.go` | Config del gateway |
| Nuevo | `src/gateway/clientregistry/clientregistry.go` | Registro concurrente de conexiones |
| Nuevo | `src/gateway/messagehandler/messagehandler.go` | Bridge protocolo externo ↔ interno |
| Nuevo | `src/common/messageprotocol/inner/inner.go` | Envelope interno JSON con `GatewayID` |
| Nuevo | `src/client/config/config.go` | Config aislada del cliente |
| Nuevo | `src/client/client/results_collector.go` | Control de EOF por query |
| Nuevo | `src/client/client/results_writer.go` | Escritura CSV de resultados |
| Nuevo | `src/serialanalyzer/**` | Nodo temporal de procesamiento serial Q1..Q5 |
| Modificado | `src/gateway/gateway/gateway.go` | Final queue por gateway, forwarding y shutdown |
| Modificado | `src/client/client/client.go` | Multi-gateway, retry, handshake, recepción por query |
| Modificado | `src/client/client/transactions.go` | Batch dinámico por bytes + validación de caso borde |
| Modificado | `src/client/client/accounts.go` | Batch dinámico por bytes + validación de caso borde |
| Modificado | `src/common/messageprotocol/external/*.go` | Mensajes de sesión, batches, query results |
| Modificado | `src/common/middleware/queue_middleware.go` | RabbitMQ quorum queues + `Body string` |
| Modificado | `Makefile` | `up/down/logs/test/switch` |
| Modificado | `docker-compose.yaml`, `scenarios/*.yaml` | Escenarios base y serial |

---

## Make y escenarios

Comandos relevantes:
- `make up`: levanta stack con `--scale gateway=<GATEWAY_AMOUNT>` leído del compose activo.
- `make down`: frena y baja contenedores.
- `make logs`: muestra logs de compose.
- `make test`: corre `go test ./...` en `src`.
- `make switch`: cambia entre `scenarios/1.yaml` y `scenarios/2_serial.yaml`.

Escenarios:
- `scenarios/1.yaml`: cliente + gateway + rabbitmq.
- `scenarios/2_serial.yaml`: 2 clientes + 2 gateways + rabbitmq + serial-analyzer.

---

## Variables de entorno relevantes

### Cliente

| Variable | Uso |
| --- | --- |
| `INPUT_TRANSACTIONS` | CSV de transacciones |
| `INPUT_ACCOUNTS` | CSV de cuentas |
| `BATCH_MAX_BYTES` | Tamaño máximo del batch binario enviado |
| `GATEWAY_PREFIX` | Prefijo DNS de gateways |
| `GATEWAY_AMOUNT` | Cantidad de gateways |
| `GATEWAY_PORT` | Puerto TCP gateway |
| `CONNECT_MAX_ATTEMPTS` | Reintentos de conexión inicial |
| `CONNECT_TIMEOUT_MS` | Timeout de dial/handshake |
| `BACKOFF_BASE_MS` | Base de backoff |
| `BACKOFF_MAX_MS` | Tope de backoff |
| `QUERY_WAIT_TIMEOUT_MS` | Timeout de espera de resultados (actual: 3600000 ms) |
| `RESULTS_DIR` | Directorio de salida de CSV |

### Gateway

| Variable | Uso |
| --- | --- |
| `ALL_TRANSACTIONS_QUEUE` | Cola de transacciones crudas |
| `ALL_ACCOUNTS_QUEUE` | Cola de cuentas crudas |
| `FINAL_QUEUE` | Prefijo de cola final (`final`) |
| `MOM_HOST`, `MOM_PORT` | RabbitMQ |
| `SERVER_HOST`, `SERVER_PORT` | Bind TCP |

### Serial-analyzer

| Variable | Uso |
| --- | --- |
| `ALL_TRANSACTIONS_QUEUE` | Cola de entrada de transacciones |
| `ALL_ACCOUNTS_QUEUE` | Cola de entrada de cuentas |
| `FINAL_QUEUE` | Prefijo de colas finales por gateway |
| `MOM_HOST`, `MOM_PORT` | RabbitMQ |

---

## Tests agregados

| Archivo | Cobertura |
| --- | --- |
| `src/common/messageprotocol/inner/inner_test.go` | Envelope interno, round-trip y casos inválidos |
| `src/gateway/messagehandler/messagehandler_test.go` | Serialización/deserialización de mensajes de gateway |
| `src/client/client/client_test.go` | Direcciones de gateways + retry/backoff |
| `src/client/client/results_test.go` | Persistencia CSV + finalización por 5 EOF |
| `src/serialanalyzer/queries/queries_test.go` | Lógica Q1..Q5 |
| `src/serialanalyzer/session/session_test.go` | Orquestación por cliente/gateway + EOF por query |

---

## Estado final

El sistema quedó preparado para validar punta a punta el TP en modo de prueba:
- ingreso por cliente,
- encolado por gateway,
- ejecución serial temporal de querys,
- retorno y persistencia de resultados por cliente,
- soporte multi-gateway sin mezcla de resultados por enrutamiento con `GatewayID`.

---

## Serial-analyzer (detalle)

`serial-analyzer` es un nodo temporal para pruebas funcionales. Su objetivo es validar que el pipeline gateway/colas/cliente funciona correctamente antes de migrar a la versión distribuida real.

Cómo trabaja:
- Consume `all_transactions` y `all_accounts`.
- Agrupa estado por sesión `(GatewayID, ClientID)`.
- Acumula batches hasta recibir EOFs.
- Emite resultados por query en formato de filas CSV dentro de `status`.
- Emite fin de query con `status="EOF"` para cada `query_id`.
- Publica cada resultado en `final_<gatewayID>` para que lo consuma sólo ese gateway.

Orden de emisión actual:
- Al recibir `EndOfTransactions`: emite Q1, Q3, Q4, Q5 (filas + EOF de cada una).
- Cuando además llega `EndOfAccounts`: emite Q2 (filas + EOF).

Criterio de finalización por sesión:
- Cuando ya emitió EOF de Q1..Q5, marca la sesión como completa y libera memoria de batches acumulados.

Limitación intencional:
- Es un nodo de validación temporal, no distribuye cómputo ni paraleliza querys por etapas.
