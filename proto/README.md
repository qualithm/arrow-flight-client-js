# Vendored Protocol Buffers

This directory contains vendored Protocol Buffer definitions from the Apache Arrow project.

## Source

- **Repository:** https://github.com/apache/arrow
- **Path:** `format/Flight.proto`, `format/FlightSql.proto`
- **Commit:** `aae49e8ba20a021d096288fef261c12d98d0a114`
- **Date:** 2026-03-02

## Files

| File                           | Description                  |
| ------------------------------ | ---------------------------- |
| `arrow/flight/Flight.proto`    | Arrow Flight RPC definitions |
| `arrow/flight/FlightSql.proto` | Arrow Flight SQL extensions  |

## Updating

To update the proto files to the latest version:

```bash
curl -sL https://raw.githubusercontent.com/apache/arrow/main/format/Flight.proto \
  -o proto/arrow/flight/Flight.proto

curl -sL https://raw.githubusercontent.com/apache/arrow/main/format/FlightSql.proto \
  -o proto/arrow/flight/FlightSql.proto
```

After updating, record the new commit SHA in this README and regenerate TypeScript code with:

```bash
bun run generate:proto
```

## License

These files are licensed under the Apache License 2.0, as noted in their headers.
