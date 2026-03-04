/**
 * Server-dependent Flight and Flight SQL Benchmarks
 *
 * These benchmarks require a running Arrow Flight or Flight SQL server.
 * They measure end-to-end performance including network latency.
 *
 * Configuration via environment variables:
 *   FLIGHT_HOST=localhost       Server host (default: localhost)
 *   FLIGHT_PORT=50051           Server port (default: 50051)
 *   FLIGHT_TLS=false            Enable TLS (default: false)
 *   FLIGHT_BEARER_TOKEN=token   Bearer token for auth (optional)
 *   FLIGHT_USERNAME=admin       Basic auth username (default: admin)
 *   FLIGHT_PASSWORD=admin123    Basic auth password (default: admin123)
 *   WARMUP_ITERATIONS=5         Warmup iterations (default: 5)
 *   BENCH_ITERATIONS=20         Benchmark iterations (default: 20)
 *   BENCH_WRITE_ROWS=1000       Rows for write benchmarks (default: 1000)
 *   BENCH_WRITES=true           Enable write benchmarks (default: true)
 *
 * @example
 *   bun run bench
 *   FLIGHT_HOST=my-server FLIGHT_PORT=443 FLIGHT_TLS=true bun run bench
 *   BENCH_WRITES=false bun run bench  # Read-only benchmarks
 */

/* eslint-disable no-console */

import { create } from "@bufbuild/protobuf"
import { type Table, tableFromArrays } from "apache-arrow"

import {
  createFlightClient,
  createFlightSqlClient,
  decodeFlightDataStream,
  decodeFlightDataToTable,
  encodeTableToFlightData,
  type FlightClient,
  type FlightDescriptorInput,
  type FlightSqlClient,
  type PreparedStatement
} from "../src/client"
import { type FlightData, FlightDescriptorSchema } from "../src/gen/arrow/flight/Flight_pb"

// ─────────────────────────────────────────────────────────────────────────────
// Configuration
// ─────────────────────────────────────────────────────────────────────────────

const serverConfig = {
  host: process.env.FLIGHT_HOST ?? "localhost",
  port: parseInt(process.env.FLIGHT_PORT ?? "50051", 10),
  tls: process.env.FLIGHT_TLS === "true",
  bearerToken: process.env.FLIGHT_BEARER_TOKEN,
  username: process.env.FLIGHT_USERNAME ?? "admin",
  password: process.env.FLIGHT_PASSWORD ?? "admin123"
}

const benchConfig = {
  warmupIterations: parseInt(process.env.WARMUP_ITERATIONS ?? "5", 10),
  benchmarkIterations: parseInt(process.env.BENCH_ITERATIONS ?? "20", 10)
}

// Test data paths (configure for your server)
const testPaths = {
  integers: ["test", "integers"],
  strings: ["test", "strings"],
  large: ["test", "large"]
}

// Test tables for Flight SQL
const testTables = {
  integers: process.env.FLIGHT_TEST_TABLE ?? "test.integers",
  large: process.env.FLIGHT_LARGE_TABLE ?? "test.large",
  writable: process.env.FLIGHT_WRITABLE_TABLE ?? "test.bench_writes"
}

// Write benchmark config
const writeConfig = {
  rowCount: parseInt(process.env.BENCH_WRITE_ROWS ?? "1000", 10),
  enabled: process.env.BENCH_WRITES !== "false"
}

// ─────────────────────────────────────────────────────────────────────────────
// Types
// ─────────────────────────────────────────────────────────────────────────────

type BenchmarkResult = {
  name: string
  category: string
  iterations: number
  totalMs: number
  avgMs: number
  minMs: number
  maxMs: number
  p50Ms: number
  p95Ms: number
  p99Ms: number
  stdDev: number
  cv: number
  throughput?: {
    rowsPerSec: number
    mbPerSec: number
  }
}

type BenchmarkSuite = {
  name: string
  results: BenchmarkResult[]
}

// ─────────────────────────────────────────────────────────────────────────────
// Statistics
// ─────────────────────────────────────────────────────────────────────────────

function calculatePercentile(sortedTimes: number[], percentile: number): number {
  const index = Math.ceil((percentile / 100) * sortedTimes.length) - 1
  return sortedTimes[Math.max(0, index)]
}

function calculateStats(times: number[]): {
  avg: number
  min: number
  max: number
  p50: number
  p95: number
  p99: number
  stdDev: number
  cv: number
} {
  const sorted = [...times].sort((a, b) => a - b)
  const avg = times.reduce((a, b) => a + b, 0) / times.length
  const min = sorted[0]
  const max = sorted[sorted.length - 1]
  const p50 = calculatePercentile(sorted, 50)
  const p95 = calculatePercentile(sorted, 95)
  const p99 = calculatePercentile(sorted, 99)
  const variance = times.reduce((sum, t) => sum + (t - avg) ** 2, 0) / times.length
  const stdDev = Math.sqrt(variance)
  const cv = avg > 0 ? (stdDev / avg) * 100 : 0

  return { avg, min, max, p50, p95, p99, stdDev, cv }
}

// ─────────────────────────────────────────────────────────────────────────────
// Benchmark Runner
// ─────────────────────────────────────────────────────────────────────────────

async function runBenchmark(
  name: string,
  category: string,
  fn: () => Promise<{ rowCount?: number; byteCount?: number }>,
  iterations: number,
  warmupIterations: number
): Promise<BenchmarkResult> {
  // Warmup phase
  for (let i = 0; i < warmupIterations; i++) {
    await fn()
  }

  // Benchmark phase
  const times: number[] = []
  let totalRows = 0
  let totalBytes = 0

  for (let i = 0; i < iterations; i++) {
    const start = performance.now()
    const result = await fn()
    const end = performance.now()
    times.push(end - start)
    totalRows += result.rowCount ?? 0
    totalBytes += result.byteCount ?? 0
  }

  const stats = calculateStats(times)
  const totalMs = times.reduce((a, b) => a + b, 0)

  const result: BenchmarkResult = {
    name,
    category,
    iterations,
    totalMs,
    avgMs: stats.avg,
    minMs: stats.min,
    maxMs: stats.max,
    p50Ms: stats.p50,
    p95Ms: stats.p95,
    p99Ms: stats.p99,
    stdDev: stats.stdDev,
    cv: stats.cv
  }

  // Calculate throughput
  if (totalRows > 0 && stats.avg > 0) {
    const avgRows = totalRows / iterations
    const avgBytes = totalBytes / iterations
    result.throughput = {
      rowsPerSec: (avgRows / stats.avg) * 1000,
      mbPerSec: (avgBytes / (1024 * 1024) / stats.avg) * 1000
    }
  }

  return result
}

// ─────────────────────────────────────────────────────────────────────────────
// Output Formatting
// ─────────────────────────────────────────────────────────────────────────────

function formatTime(ms: number): string {
  if (ms < 0.001) {
    return `${(ms * 1_000_000).toFixed(1)}ns`
  }
  if (ms < 1) {
    return `${(ms * 1000).toFixed(2)}μs`
  }
  if (ms < 1000) {
    return `${ms.toFixed(2)}ms`
  }
  return `${(ms / 1000).toFixed(2)}s`
}

function formatNumber(n: number): string {
  if (n >= 1_000_000) {
    return `${(n / 1_000_000).toFixed(2)}M`
  }
  if (n >= 1_000) {
    return `${(n / 1_000).toFixed(2)}K`
  }
  return n.toFixed(2)
}

function printResult(result: BenchmarkResult): void {
  console.log(`  ${result.name}:`)
  console.log(
    `    Avg: ${formatTime(result.avgMs)}  P50: ${formatTime(result.p50Ms)}  P95: ${formatTime(result.p95Ms)}  P99: ${formatTime(result.p99Ms)}`
  )
  console.log(
    `    Min: ${formatTime(result.minMs)}  Max: ${formatTime(result.maxMs)}  CV: ${result.cv.toFixed(1)}%`
  )
  if (result.throughput) {
    const { rowsPerSec, mbPerSec } = result.throughput
    if (rowsPerSec > 0) {
      console.log(`    Throughput: ${formatNumber(rowsPerSec)} rows/s  ${mbPerSec.toFixed(2)} MB/s`)
    }
  }
}

function printSummaryTable(suites: BenchmarkSuite[]): void {
  console.log(`\n${"═".repeat(100)}`)
  console.log("SUMMARY")
  console.log("═".repeat(100))

  const headers = ["Benchmark", "Avg", "P50", "P95", "P99", "CV", "Throughput"]
  const widths = [45, 12, 12, 12, 12, 8, 15]

  console.log(headers.map((h, i) => h.padEnd(widths[i])).join(""))
  console.log("─".repeat(100))

  for (const suite of suites) {
    console.log(`\n${suite.name}`)
    for (const r of suite.results) {
      const throughput = r.throughput
        ? r.throughput.rowsPerSec > 0
          ? `${formatNumber(r.throughput.rowsPerSec)}/s`
          : `${r.throughput.mbPerSec.toFixed(1)} MB/s`
        : "-"

      const row = [
        `  ${r.name}`.slice(0, widths[0] - 1).padEnd(widths[0]),
        formatTime(r.avgMs).padEnd(widths[1]),
        formatTime(r.p50Ms).padEnd(widths[2]),
        formatTime(r.p95Ms).padEnd(widths[3]),
        formatTime(r.p99Ms).padEnd(widths[4]),
        `${r.cv.toFixed(1)}%`.padEnd(widths[5]),
        throughput.padEnd(widths[6])
      ]
      console.log(row.join(""))
    }
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// Client Setup
// ─────────────────────────────────────────────────────────────────────────────

function getServerUrl(): string {
  const protocol = serverConfig.tls ? "https" : "http"
  return `${protocol}://${serverConfig.host}:${String(serverConfig.port)}`
}

function getAuth():
  | { type: "bearer"; token: string }
  | { type: "basic"; credentials: { username: string; password: string } } {
  if (serverConfig.bearerToken !== undefined && serverConfig.bearerToken !== "") {
    return { type: "bearer", token: serverConfig.bearerToken }
  }
  return {
    type: "basic",
    credentials: { username: serverConfig.username, password: serverConfig.password }
  }
}

async function createAndAuthenticateFlightClient(): Promise<FlightClient> {
  const client = createFlightClient({ url: getServerUrl(), auth: getAuth() })
  if (serverConfig.bearerToken === undefined || serverConfig.bearerToken === "") {
    await client.authenticate()
  }
  return client
}

async function createAndAuthenticateFlightSqlClient(): Promise<FlightSqlClient> {
  const client = createFlightSqlClient({ url: getServerUrl(), auth: getAuth() })
  if (serverConfig.bearerToken === undefined || serverConfig.bearerToken === "") {
    await client.authenticate()
  }
  return client
}

// ─────────────────────────────────────────────────────────────────────────────
// Flight Client Benchmarks
// ─────────────────────────────────────────────────────────────────────────────

function pathDescriptor(...path: string[]): FlightDescriptorInput {
  return { type: "path", path }
}

/**
 * Generate test data for write benchmarks.
 */
function generateWriteData(rowCount: number): Table {
  const ids = new Int32Array(rowCount)
  const values = new Float64Array(rowCount)
  const names: string[] = []

  for (let i = 0; i < rowCount; i++) {
    ids[i] = i
    values[i] = Math.random() * 1000
    names.push(`row_${String(i)}`)
  }

  return tableFromArrays({
    id: ids,
    value: values,
    name: names
  })
}

/**
 * Wrap FlightData with a descriptor for doPut.
 */
async function* withDescriptor(
  data: AsyncIterable<FlightData>,
  descriptor: FlightDescriptorInput
): AsyncIterable<FlightData> {
  // Convert FlightDescriptorInput to proto FlightDescriptor
  const protoDescriptor =
    descriptor.type === "path"
      ? create(FlightDescriptorSchema, { type: 1, path: descriptor.path })
      : create(FlightDescriptorSchema, { type: 2, cmd: descriptor.cmd })

  let first = true
  for await (const chunk of data) {
    if (first) {
      yield { ...chunk, flightDescriptor: protoDescriptor }
      first = false
    } else {
      yield chunk
    }
  }
}

async function benchmarkFlightClient(): Promise<BenchmarkSuite> {
  const results: BenchmarkResult[] = []
  const category = "FlightClient"

  console.log(`\n${"─".repeat(60)}`)
  console.log("FLIGHT CLIENT BENCHMARKS")
  console.log("─".repeat(60))

  const client = await createAndAuthenticateFlightClient()

  try {
    // listFlights benchmark
    console.log("\n[listFlights]")
    const listResult = await runBenchmark(
      "listFlights",
      category,
      async () => {
        let count = 0
        for await (const _flight of client.listFlights()) {
          count++
        }
        return { rowCount: count }
      },
      benchConfig.benchmarkIterations,
      benchConfig.warmupIterations
    )
    results.push(listResult)
    printResult(listResult)

    // getFlightInfo benchmark
    console.log("\n[getFlightInfo]")
    const descriptor = pathDescriptor(...testPaths.integers)
    const infoResult = await runBenchmark(
      "getFlightInfo",
      category,
      async () => {
        const info = await client.getFlightInfo(descriptor)
        return { rowCount: Number(info.totalRecords) }
      },
      benchConfig.benchmarkIterations,
      benchConfig.warmupIterations
    )
    results.push(infoResult)
    printResult(infoResult)

    // doGet benchmark (small dataset)
    console.log("\n[doGet - small]")
    const infoSmall = await client.getFlightInfo(pathDescriptor(...testPaths.integers))
    if (infoSmall.endpoint.length > 0 && infoSmall.endpoint[0].ticket) {
      const ticketSmall = infoSmall.endpoint[0].ticket
      const doGetSmallResult = await runBenchmark(
        "doGet (integers)",
        category,
        async () => {
          let rowCount = 0
          let byteCount = 0
          for await (const batch of decodeFlightDataStream(client.doGet(ticketSmall))) {
            rowCount += batch.numRows
            byteCount += batch.numCols * batch.numRows * 8 // Estimate
          }
          return { rowCount, byteCount }
        },
        benchConfig.benchmarkIterations,
        benchConfig.warmupIterations
      )
      results.push(doGetSmallResult)
      printResult(doGetSmallResult)
    }

    // doGet benchmark (large dataset)
    console.log("\n[doGet - large]")
    try {
      const infoLarge = await client.getFlightInfo(pathDescriptor(...testPaths.large))
      if (infoLarge.endpoint.length > 0 && infoLarge.endpoint[0].ticket) {
        const ticketLarge = infoLarge.endpoint[0].ticket
        const doGetLargeResult = await runBenchmark(
          "doGet (large)",
          category,
          async () => {
            let rowCount = 0
            let byteCount = 0
            for await (const batch of decodeFlightDataStream(client.doGet(ticketLarge))) {
              rowCount += batch.numRows
              byteCount += batch.numCols * batch.numRows * 8
            }
            return { rowCount, byteCount }
          },
          benchConfig.benchmarkIterations,
          benchConfig.warmupIterations
        )
        results.push(doGetLargeResult)
        printResult(doGetLargeResult)
      }
    } catch {
      console.log("  (large dataset not available, skipping)")
    }

    // doGet to Table benchmark
    console.log("\n[doGet → Table]")
    if (infoSmall.endpoint.length > 0 && infoSmall.endpoint[0].ticket) {
      const { ticket } = infoSmall.endpoint[0]
      const toTableResult = await runBenchmark(
        "doGet → decodeToTable",
        category,
        async () => {
          const table = await decodeFlightDataToTable(client.doGet(ticket))
          return { rowCount: table.numRows, byteCount: table.numRows * table.numCols * 8 }
        },
        benchConfig.benchmarkIterations,
        benchConfig.warmupIterations
      )
      results.push(toTableResult)
      printResult(toTableResult)
    }

    // doPut benchmark (write)
    if (writeConfig.enabled) {
      console.log("\n[doPut - write]")
      const writeDescriptor = pathDescriptor("bench", "write_test")
      const writeTable = generateWriteData(writeConfig.rowCount)
      const dataBytes = writeConfig.rowCount * 20 // Estimate ~20 bytes/row

      try {
        const doPutResult = await runBenchmark(
          `doPut (${String(writeConfig.rowCount)} rows)`,
          category,
          async () => {
            const flightData = encodeTableToFlightData(writeTable)
            let _putCount = 0
            for await (const _result of client.doPut(withDescriptor(flightData, writeDescriptor))) {
              _putCount++
            }
            return { rowCount: writeConfig.rowCount, byteCount: dataBytes }
          },
          benchConfig.benchmarkIterations,
          benchConfig.warmupIterations
        )
        results.push(doPutResult)
        printResult(doPutResult)
      } catch {
        console.log("  (doPut not supported or permission denied, skipping)")
      }
    }
  } finally {
    client.close()
  }

  return { name: "FlightClient", results }
}

// ─────────────────────────────────────────────────────────────────────────────
// Flight SQL Client Benchmarks
// ─────────────────────────────────────────────────────────────────────────────

async function benchmarkFlightSqlClient(): Promise<BenchmarkSuite> {
  const results: BenchmarkResult[] = []
  const category = "FlightSqlClient"

  console.log(`\n${"─".repeat(60)}`)
  console.log("FLIGHT SQL CLIENT BENCHMARKS")
  console.log("─".repeat(60))

  const client = await createAndAuthenticateFlightSqlClient()

  try {
    // Simple query benchmark
    console.log("\n[query - small]")
    const queryResult = await runBenchmark(
      "query (SELECT *)",
      category,
      async () => {
        const table = await client.query(`SELECT * FROM ${testTables.integers}`)
        return { rowCount: table.numRows, byteCount: table.numRows * table.numCols * 8 }
      },
      benchConfig.benchmarkIterations,
      benchConfig.warmupIterations
    )
    results.push(queryResult)
    printResult(queryResult)

    // Large query benchmark
    console.log("\n[query - large]")
    try {
      const largeQueryResult = await runBenchmark(
        "query (large table)",
        category,
        async () => {
          const table = await client.query(`SELECT * FROM ${testTables.large}`)
          return { rowCount: table.numRows, byteCount: table.numRows * table.numCols * 8 }
        },
        benchConfig.benchmarkIterations,
        benchConfig.warmupIterations
      )
      results.push(largeQueryResult)
      printResult(largeQueryResult)
    } catch {
      console.log("  (large table not available, skipping)")
    }

    // Query with streaming (queryBatches)
    console.log("\n[queryBatches]")
    const batchResult = await runBenchmark(
      "queryBatches",
      category,
      async () => {
        let rowCount = 0
        let byteCount = 0
        for await (const batch of client.queryBatches(`SELECT * FROM ${testTables.integers}`)) {
          rowCount += batch.numRows
          byteCount += batch.numCols * batch.numRows * 8
        }
        return { rowCount, byteCount }
      },
      benchConfig.benchmarkIterations,
      benchConfig.warmupIterations
    )
    results.push(batchResult)
    printResult(batchResult)

    // Prepared statement benchmark
    console.log("\n[preparedStatement]")
    const preparedResult = await runBenchmark(
      "prepared statement",
      category,
      async () => {
        const stmt = await client.prepare(`SELECT * FROM ${testTables.integers}`)
        try {
          const table = await client.executePrepared(stmt)
          return { rowCount: table.numRows, byteCount: table.numRows * table.numCols * 8 }
        } finally {
          await client.closePreparedStatement(stmt)
        }
      },
      benchConfig.benchmarkIterations,
      benchConfig.warmupIterations
    )
    results.push(preparedResult)
    printResult(preparedResult)

    // Prepared statement reuse benchmark
    console.log("\n[prepared reuse]")
    const stmt: PreparedStatement = await client.prepare(`SELECT * FROM ${testTables.integers}`)
    try {
      const reuseResult = await runBenchmark(
        "prepared (reuse)",
        category,
        async () => {
          const table = await client.executePrepared(stmt)
          return { rowCount: table.numRows, byteCount: table.numRows * table.numCols * 8 }
        },
        benchConfig.benchmarkIterations,
        benchConfig.warmupIterations
      )
      results.push(reuseResult)
      printResult(reuseResult)
    } finally {
      await client.closePreparedStatement(stmt)
    }

    // Metadata queries benchmark
    console.log("\n[metadata]")
    const catalogsResult = await runBenchmark(
      "getCatalogs",
      category,
      async () => {
        const table = await client.getCatalogs()
        return { rowCount: table.numRows }
      },
      benchConfig.benchmarkIterations,
      benchConfig.warmupIterations
    )
    results.push(catalogsResult)
    printResult(catalogsResult)

    const schemasResult = await runBenchmark(
      "getDbSchemas",
      category,
      async () => {
        const table = await client.getDbSchemas()
        return { rowCount: table.numRows }
      },
      benchConfig.benchmarkIterations,
      benchConfig.warmupIterations
    )
    results.push(schemasResult)
    printResult(schemasResult)

    const tablesResult = await runBenchmark(
      "getTables",
      category,
      async () => {
        const table = await client.getTables()
        return { rowCount: table.numRows }
      },
      benchConfig.benchmarkIterations,
      benchConfig.warmupIterations
    )
    results.push(tablesResult)
    printResult(tablesResult)

    // executeUpdate benchmark (write)
    if (writeConfig.enabled) {
      console.log("\n[executeUpdate - write]")
      try {
        // Create table if needed (ignore errors)
        try {
          await client.executeUpdate(
            `CREATE TABLE IF NOT EXISTS ${testTables.writable} (id INT, value DOUBLE, name VARCHAR)`
          )
        } catch {
          // Table may already exist or syntax not supported
        }

        const insertResult = await runBenchmark(
          "executeUpdate (INSERT)",
          category,
          async () => {
            const result = await client.executeUpdate(
              `INSERT INTO ${testTables.writable} (id, value, name) VALUES (1, 3.14, 'bench')`
            )
            return { rowCount: Number(result.recordCount) }
          },
          benchConfig.benchmarkIterations,
          benchConfig.warmupIterations
        )
        results.push(insertResult)
        printResult(insertResult)

        const updateResult = await runBenchmark(
          "executeUpdate (UPDATE)",
          category,
          async () => {
            const result = await client.executeUpdate(
              `UPDATE ${testTables.writable} SET value = 2.71 WHERE id = 1`
            )
            return { rowCount: Number(result.recordCount) }
          },
          benchConfig.benchmarkIterations,
          benchConfig.warmupIterations
        )
        results.push(updateResult)
        printResult(updateResult)

        const deleteResult = await runBenchmark(
          "executeUpdate (DELETE)",
          category,
          async () => {
            const result = await client.executeUpdate(
              `DELETE FROM ${testTables.writable} WHERE id = 1`
            )
            return { rowCount: Number(result.recordCount) }
          },
          benchConfig.benchmarkIterations,
          benchConfig.warmupIterations
        )
        results.push(deleteResult)
        printResult(deleteResult)
      } catch {
        console.log("  (executeUpdate not supported or permission denied, skipping)")
      }
    }
  } finally {
    client.close()
  }

  return { name: "FlightSqlClient", results }
}

// ─────────────────────────────────────────────────────────────────────────────
// Main
// ─────────────────────────────────────────────────────────────────────────────

async function main(): Promise<void> {
  console.log("╔════════════════════════════════════════════════════════════════╗")
  console.log("║     Arrow Flight/SQL Server Benchmarks                         ║")
  console.log("╚════════════════════════════════════════════════════════════════╝")
  console.log()
  console.log(`Server: ${getServerUrl()}`)
  console.log(
    `Auth: ${serverConfig.bearerToken !== undefined && serverConfig.bearerToken !== "" ? "Bearer token" : "Basic auth"}`
  )
  console.log(`Warmup: ${String(benchConfig.warmupIterations)} iterations`)
  console.log(`Benchmark: ${String(benchConfig.benchmarkIterations)} iterations`)

  const suites: BenchmarkSuite[] = []

  try {
    // Test connectivity first
    console.log("\nTesting server connectivity...")
    const testClient = await createAndAuthenticateFlightClient()
    testClient.close()
    console.log("Connected successfully.")

    // Run benchmarks
    suites.push(await benchmarkFlightClient())
    suites.push(await benchmarkFlightSqlClient())

    // Print summary
    printSummaryTable(suites)

    console.log(`\n${"═".repeat(100)}`)
    console.log("Server benchmarks complete.")
  } catch (error) {
    console.error("\nFailed to connect to server:")
    console.error(error instanceof Error ? error.message : String(error))
    console.error("\nMake sure a Flight SQL server is running and configured correctly.")
    console.error("Environment variables:")
    console.error("  FLIGHT_HOST, FLIGHT_PORT, FLIGHT_TLS, FLIGHT_BEARER_TOKEN")
    console.error("  FLIGHT_USERNAME, FLIGHT_PASSWORD")
    process.exit(1)
  }
}

main().catch(console.error)
