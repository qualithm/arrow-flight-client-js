# Examples

Runnable examples demonstrating Arrow Flight client usage.

## Prerequisites

Examples require a running Arrow Flight or Flight SQL server.

## Environment Variables

| Variable              | Default     | Description                     |
| --------------------- | ----------- | ------------------------------- |
| `FLIGHT_HOST`         | `localhost` | Server host                     |
| `FLIGHT_PORT`         | `50051`     | Server port                     |
| `FLIGHT_TLS`          | `false`     | Enable TLS                      |
| `FLIGHT_BEARER_TOKEN` | —           | Bearer token for authentication |

## Running Examples

```bash
bun run examples/flight-client.ts
bun run examples/flight-sql-client.ts
```

## Example Files

| File                                         | Description                                      |
| -------------------------------------------- | ------------------------------------------------ |
| [flight-client.ts](flight-client.ts)         | FlightClient: list flights, doGet, doPut         |
| [flight-sql-client.ts](flight-sql-client.ts) | FlightSqlClient: queries, transactions, metadata |
