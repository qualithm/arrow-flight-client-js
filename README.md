# Arrow Flight Client

[![CI](https://github.com/qualithm/arrow-flight-client-js/actions/workflows/ci.yaml/badge.svg)](https://github.com/qualithm/arrow-flight-client-js/actions/workflows/ci.yaml)
[![codecov](https://codecov.io/gh/qualithm/arrow-flight-client-js/graph/badge.svg)](https://codecov.io/gh/qualithm/arrow-flight-client-js)
[![npm](https://img.shields.io/npm/v/@qualithm/arrow-flight-client)](https://www.npmjs.com/package/@qualithm/arrow-flight-client)

Unified Arrow Flight client for JavaScript and TypeScript runtimes.

## Development

### Prerequisites

- [Bun](https://bun.sh/) (recommended), Node.js 20+, or [Deno](https://deno.land/)

### Setup

```bash
bun install
```

### Building

```bash
bun run build
```

### Running

```bash
bun run start
```

### Testing

```bash
bun test
```

### Linting & Formatting

```bash
bun run lint
bun run format
bun run typecheck
```

### Benchmarks

```bash
bun run bench
```

## Publishing

The package is automatically published to NPM when CI passes on main. Update the version in
`package.json` before merging to trigger a new release.

## Licence

Apache-2.0
