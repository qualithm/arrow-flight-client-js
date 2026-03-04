# Examples

This directory contains runnable examples demonstrating Arrow Flight client usage.

## Prerequisites

Examples require a running Arrow Flight or Flight SQL server. Configure connection via environment
variables:

- `FLIGHT_HOST`: Server host (default: `localhost`)
- `FLIGHT_PORT`: Server port (default: `50051`)
- `FLIGHT_TLS`: Enable TLS (default: `false`)
- `FLIGHT_BEARER_TOKEN`: Bearer token for authentication (optional)

## Running Examples

```bash
# FlightClient - core Flight RPC operations
bun run examples/flight-client.ts

# FlightSqlClient - SQL queries, transactions, metadata
bun run examples/flight-sql-client.ts
```

## Example Files

| File                                         | Description                                      |
| -------------------------------------------- | ------------------------------------------------ |
| [flight-client.ts](flight-client.ts)         | FlightClient: list flights, doGet, doPut         |
| [flight-sql-client.ts](flight-sql-client.ts) | FlightSqlClient: queries, transactions, metadata |
