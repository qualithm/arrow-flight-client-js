/**
 * Arrow Flight Client Benchmarks
 *
 * Comprehensive benchmarks for read and write operations:
 * - Arrow IPC encoding (writes)
 * - Arrow IPC decoding (reads)
 * - Column type performance
 * - Batch strategy impact
 *
 * Run with: bun run bench
 *
 * Configuration via environment variables:
 *   WARMUP_ITERATIONS=10  Number of warmup iterations (default: 10)
 *   BENCH_ITERATIONS=100  Number of benchmark iterations (default: 100)
 *   BENCH_SIZES=small,medium,large  Sizes to benchmark (default: all)
 *
 * @example
 *   bun run bench
 *   BENCH_ITERATIONS=50 bun run bench
 *   BENCH_SIZES=small,medium bun run bench
 */

/* eslint-disable no-console */

import {
  RecordBatchReader,
  RecordBatchStreamWriter,
  type Table,
  tableFromArrays
} from "apache-arrow"

import {
  createFlightDataFromIpc,
  decodeFlightDataStream,
  decodeFlightDataToTable,
  encodeRecordBatchesToFlightData,
  encodeTableToFlightData,
  parseIpcMessage
} from "../src/client/ipc"
import type { FlightData } from "../src/gen/arrow/flight/Flight_pb"

// ─────────────────────────────────────────────────────────────────────────────
// Configuration
// ─────────────────────────────────────────────────────────────────────────────

const config = {
  warmupIterations: parseInt(process.env.WARMUP_ITERATIONS ?? "10", 10),
  benchmarkIterations: parseInt(process.env.BENCH_ITERATIONS ?? "100", 10),
  sizes: (process.env.BENCH_SIZES ?? "small,medium,large").split(",") as (
    | "small"
    | "medium"
    | "large"
  )[]
}

// Row counts for each size category
const ROW_COUNTS = {
  small: 100,
  medium: 10_000,
  large: 1_000_000
} as const

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
  cv: number // coefficient of variation (%)
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
// Test Data Generation
// ─────────────────────────────────────────────────────────────────────────────

/**
 * Generate test data with Int32 columns only.
 */
function generateIntTable(rowCount: number): Table {
  const id = new Int32Array(rowCount)
  const value1 = new Int32Array(rowCount)
  const value2 = new Int32Array(rowCount)

  for (let i = 0; i < rowCount; i++) {
    id[i] = i
    value1[i] = i * 2
    value2[i] = i * 3
  }

  return tableFromArrays({ id, value1, value2 })
}

/**
 * Generate test data with Float64 columns only.
 */
function generateFloatTable(rowCount: number): Table {
  const id = new Float64Array(rowCount)
  const value1 = new Float64Array(rowCount)
  const value2 = new Float64Array(rowCount)

  for (let i = 0; i < rowCount; i++) {
    id[i] = i
    value1[i] = i * 1.1
    value2[i] = i * 2.2
  }

  return tableFromArrays({ id, value1, value2 })
}

/**
 * Generate test data with string columns.
 */
function generateStringTable(rowCount: number): Table {
  const names: string[] = []
  const descriptions: string[] = []

  for (let i = 0; i < rowCount; i++) {
    names.push(`name_${String(i)}`)
    descriptions.push(`This is a longer description for item number ${String(i)}`)
  }

  return tableFromArrays({ name: names, description: descriptions })
}

/**
 * Generate test data with mixed column types.
 */
function generateMixedTable(rowCount: number): Table {
  const id = new Int32Array(rowCount)
  const value = new Float64Array(rowCount)
  const names: string[] = []

  for (let i = 0; i < rowCount; i++) {
    id[i] = i
    value[i] = i * 1.5
    names.push(`item_${String(i)}`)
  }

  return tableFromArrays({ id, value, name: names })
}

/**
 * Calculate the approximate size of a table in bytes.
 * This is a rough estimate based on row count and column types.
 */
function estimateTableSize(table: Table): number {
  let size = 0
  for (const batch of table.batches) {
    // Estimate: numRows * numCols * 8 bytes (assumes 64-bit values on average)
    size += batch.numRows * batch.numCols * 8
  }
  return size
}

// ─────────────────────────────────────────────────────────────────────────────
// FlightData Utilities
// ─────────────────────────────────────────────────────────────────────────────

/**
 * Collect FlightData from an async generator into an array.
 */
async function collectFlightData(
  generator: AsyncGenerator<FlightData>
): Promise<{ data: FlightData[]; totalBytes: number }> {
  const data: FlightData[] = []
  let totalBytes = 0

  for await (const fd of generator) {
    data.push(fd)
    totalBytes += fd.dataHeader.length + fd.dataBody.length
  }

  return { data, totalBytes }
}

/**
 * Create an async iterable from an array.
 */
// eslint-disable-next-line @typescript-eslint/require-await
async function* asyncIterable<T>(items: T[]): AsyncIterable<T> {
  for (const item of items) {
    yield item
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// Benchmark Runner
// ─────────────────────────────────────────────────────────────────────────────

type AsyncBenchmarkOptions = {
  name: string
  category: string
  fn: () => Promise<void>
  iterations: number
  warmupIterations: number
  rowCount?: number
  dataBytes?: number
}

async function runAsyncBenchmark(options: AsyncBenchmarkOptions): Promise<BenchmarkResult> {
  const { name, category, fn, iterations, warmupIterations, rowCount, dataBytes } = options

  // Warmup phase
  for (let i = 0; i < warmupIterations; i++) {
    await fn()
  }

  // Benchmark phase - measure each iteration individually
  const times: number[] = []

  for (let i = 0; i < iterations; i++) {
    const start = performance.now()
    await fn()
    const end = performance.now()
    times.push(end - start)
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

  // Calculate throughput if row count and data size provided
  if (rowCount !== undefined && dataBytes !== undefined && stats.avg > 0) {
    const rowsPerSec = (rowCount / stats.avg) * 1000
    const mbPerSec = (dataBytes / (1024 * 1024) / stats.avg) * 1000
    result.throughput = { rowsPerSec, mbPerSec }
  }

  return result
}

type SyncBenchmarkOptions = {
  name: string
  category: string
  fn: () => void
  iterations: number
  warmupIterations: number
  dataBytes?: number
}

function runSyncBenchmark(options: SyncBenchmarkOptions): BenchmarkResult {
  const { name, category, fn, iterations, warmupIterations, dataBytes } = options

  // Warmup phase
  for (let i = 0; i < warmupIterations; i++) {
    fn()
  }

  // Benchmark phase
  const times: number[] = []

  for (let i = 0; i < iterations; i++) {
    const start = performance.now()
    fn()
    const end = performance.now()
    times.push(end - start)
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

  if (dataBytes !== undefined && stats.avg > 0) {
    const mbPerSec = (dataBytes / (1024 * 1024) / stats.avg) * 1000
    result.throughput = { rowsPerSec: 0, mbPerSec }
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

function formatBytes(bytes: number): string {
  if (bytes >= 1024 * 1024 * 1024) {
    return `${(bytes / (1024 * 1024 * 1024)).toFixed(2)} GB`
  }
  if (bytes >= 1024 * 1024) {
    return `${(bytes / (1024 * 1024)).toFixed(2)} MB`
  }
  if (bytes >= 1024) {
    return `${(bytes / 1024).toFixed(2)} KB`
  }
  return `${String(bytes)} B`
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
    } else {
      console.log(`    Throughput: ${mbPerSec.toFixed(2)} MB/s`)
    }
  }
}

function printSummaryTable(suites: BenchmarkSuite[]): void {
  console.log(`\n${"═".repeat(100)}`)
  console.log("SUMMARY")
  console.log("═".repeat(100))

  const headers = ["Benchmark", "Avg", "P50", "P95", "P99", "CV", "Throughput"]
  const widths = [45, 12, 12, 12, 12, 8, 15]

  // Print header
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
// Benchmark Suites
// ─────────────────────────────────────────────────────────────────────────────

/**
 * Benchmark Arrow IPC encoding (Write path).
 */
async function benchmarkEncoding(): Promise<BenchmarkSuite> {
  const results: BenchmarkResult[] = []
  const category = "Encoding"

  console.log(`\n${"─".repeat(60)}`)
  console.log("ENCODING BENCHMARKS (Write Path)")
  console.log("─".repeat(60))

  for (const size of config.sizes) {
    const rowCount = ROW_COUNTS[size]
    const table = generateMixedTable(rowCount)
    const dataBytes = estimateTableSize(table)

    console.log(`\n[${size}] ${rowCount.toLocaleString()} rows, ~${formatBytes(dataBytes)}`)

    // encodeTableToFlightData
    const encodeTableResult = await runAsyncBenchmark({
      name: `encodeTableToFlightData (${size})`,
      category,
      fn: async () => {
        const { data } = await collectFlightData(encodeTableToFlightData(table))
        // Prevent dead code elimination
        if (data.length === 0) {
          throw new Error("unexpected")
        }
      },
      iterations: config.benchmarkIterations,
      warmupIterations: config.warmupIterations,
      rowCount,
      dataBytes
    })
    results.push(encodeTableResult)
    printResult(encodeTableResult)

    // encodeRecordBatchesToFlightData
    const { batches } = table
    const { schema } = table
    const encodeBatchesResult = await runAsyncBenchmark({
      name: `encodeRecordBatchesToFlightData (${size})`,
      category,
      fn: async () => {
        const { data } = await collectFlightData(encodeRecordBatchesToFlightData(batches, schema))
        if (data.length === 0) {
          throw new Error("unexpected")
        }
      },
      iterations: config.benchmarkIterations,
      warmupIterations: config.warmupIterations,
      rowCount,
      dataBytes
    })
    results.push(encodeBatchesResult)
    printResult(encodeBatchesResult)

    // Raw RecordBatchStreamWriter (baseline)
    const rawEncodeResult = runSyncBenchmark({
      name: `RecordBatchStreamWriter (${size})`,
      category,
      fn: () => {
        const writer = new RecordBatchStreamWriter()
        writer.reset(undefined, schema)
        for (const batch of batches) {
          writer.write(batch)
        }
        writer.finish()
        const bytes = writer.toUint8Array(true)
        if (bytes.length === 0) {
          throw new Error("unexpected")
        }
      },
      iterations: config.benchmarkIterations,
      warmupIterations: config.warmupIterations,
      dataBytes
    })
    results.push(rawEncodeResult)
    printResult(rawEncodeResult)
  }

  return { name: "Encoding (Write)", results }
}

/**
 * Benchmark Arrow IPC decoding (Read path).
 */
async function benchmarkDecoding(): Promise<BenchmarkSuite> {
  const results: BenchmarkResult[] = []
  const category = "Decoding"

  console.log(`\n${"─".repeat(60)}`)
  console.log("DECODING BENCHMARKS (Read Path)")
  console.log("─".repeat(60))

  for (const size of config.sizes) {
    const rowCount = ROW_COUNTS[size]
    const table = generateMixedTable(rowCount)

    // Pre-encode to FlightData for decoding benchmarks
    const { data: flightData, totalBytes } = await collectFlightData(encodeTableToFlightData(table))

    console.log(
      `\n[${size}] ${rowCount.toLocaleString()} rows, ~${formatBytes(totalBytes)} encoded`
    )

    // decodeFlightDataStream
    const decodeStreamResult = await runAsyncBenchmark({
      name: `decodeFlightDataStream (${size})`,
      category,
      fn: async () => {
        let batchCount = 0
        for await (const batch of decodeFlightDataStream(asyncIterable(flightData))) {
          batchCount += batch.numRows
        }
        if (batchCount === 0 && rowCount > 0) {
          throw new Error("unexpected")
        }
      },
      iterations: config.benchmarkIterations,
      warmupIterations: config.warmupIterations,
      rowCount,
      dataBytes: totalBytes
    })
    results.push(decodeStreamResult)
    printResult(decodeStreamResult)

    // decodeFlightDataToTable
    const decodeTableResult = await runAsyncBenchmark({
      name: `decodeFlightDataToTable (${size})`,
      category,
      fn: async () => {
        const decoded = await decodeFlightDataToTable(asyncIterable(flightData))
        if (decoded.numRows === 0 && rowCount > 0) {
          throw new Error("unexpected")
        }
      },
      iterations: config.benchmarkIterations,
      warmupIterations: config.warmupIterations,
      rowCount,
      dataBytes: totalBytes
    })
    results.push(decodeTableResult)
    printResult(decodeTableResult)

    // Raw RecordBatchReader (baseline)
    // Create raw IPC bytes
    const writer = new RecordBatchStreamWriter()
    writer.reset(undefined, table.schema)
    for (const batch of table.batches) {
      writer.write(batch)
    }
    writer.finish()
    const ipcBytes = writer.toUint8Array(true)

    const rawDecodeResult = runSyncBenchmark({
      name: `RecordBatchReader.from (${size})`,
      category,
      fn: () => {
        const reader = RecordBatchReader.from(ipcBytes)
        let totalRows = 0
        for (const batch of reader) {
          totalRows += batch.numRows
        }
        if (totalRows === 0 && rowCount > 0) {
          throw new Error("unexpected")
        }
      },
      iterations: config.benchmarkIterations,
      warmupIterations: config.warmupIterations,
      dataBytes: ipcBytes.length
    })
    results.push(rawDecodeResult)
    printResult(rawDecodeResult)
  }

  return { name: "Decoding (Read)", results }
}

/**
 * Benchmark different column types.
 */
async function benchmarkColumnTypes(): Promise<BenchmarkSuite> {
  const results: BenchmarkResult[] = []
  const category = "Column Types"
  const rowCount = ROW_COUNTS.medium // Use medium size for type comparison

  console.log(`\n${"─".repeat(60)}`)
  console.log("COLUMN TYPE BENCHMARKS")
  console.log("─".repeat(60))
  console.log(`Using ${rowCount.toLocaleString()} rows for each type`)

  const tableGenerators: { name: string; generator: (n: number) => Table }[] = [
    { name: "Int32", generator: generateIntTable },
    { name: "Float64", generator: generateFloatTable },
    { name: "String", generator: generateStringTable },
    { name: "Mixed", generator: generateMixedTable }
  ]

  for (const { name, generator } of tableGenerators) {
    const table = generator(rowCount)
    const dataBytes = estimateTableSize(table)

    console.log(`\n[${name}] ~${formatBytes(dataBytes)}`)

    // Encoding
    const encodeResult = await runAsyncBenchmark({
      name: `Encode ${name}`,
      category,
      fn: async () => {
        const { data } = await collectFlightData(encodeTableToFlightData(table))
        if (data.length === 0) {
          throw new Error("unexpected")
        }
      },
      iterations: config.benchmarkIterations,
      warmupIterations: config.warmupIterations,
      rowCount,
      dataBytes
    })
    results.push(encodeResult)
    printResult(encodeResult)

    // Decoding
    const { data: flightData, totalBytes } = await collectFlightData(encodeTableToFlightData(table))

    const decodeResult = await runAsyncBenchmark({
      name: `Decode ${name}`,
      category,
      fn: async () => {
        const decoded = await decodeFlightDataToTable(asyncIterable(flightData))
        if (decoded.numRows === 0) {
          throw new Error("unexpected")
        }
      },
      iterations: config.benchmarkIterations,
      warmupIterations: config.warmupIterations,
      rowCount,
      dataBytes: totalBytes
    })
    results.push(decodeResult)
    printResult(decodeResult)
  }

  return { name: "Column Types", results }
}

/**
 * Benchmark full round-trip (encode + decode cycle).
 */
async function benchmarkRoundTrip(): Promise<BenchmarkSuite> {
  const results: BenchmarkResult[] = []
  const category = "Round-Trip"

  console.log(`\n${"─".repeat(60)}`)
  console.log("ROUND-TRIP BENCHMARKS (Encode + Decode)")
  console.log("─".repeat(60))

  for (const size of config.sizes) {
    const rowCount = ROW_COUNTS[size]
    const table = generateMixedTable(rowCount)
    const dataBytes = estimateTableSize(table)

    console.log(`\n[${size}] ${rowCount.toLocaleString()} rows, ~${formatBytes(dataBytes)}`)

    // Full round-trip: encode then decode
    const roundTripResult = await runAsyncBenchmark({
      name: `Round-trip (${size})`,
      category,
      fn: async () => {
        // Encode
        const flightData: FlightData[] = []
        for await (const fd of encodeTableToFlightData(table)) {
          flightData.push(fd)
        }
        // Decode
        const decoded = await decodeFlightDataToTable(asyncIterable(flightData))
        if (decoded.numRows !== table.numRows) {
          throw new Error("row count mismatch")
        }
      },
      iterations: config.benchmarkIterations,
      warmupIterations: config.warmupIterations,
      rowCount,
      dataBytes
    })
    results.push(roundTripResult)
    printResult(roundTripResult)
  }

  // Also test different column types at medium size
  const rowCount = ROW_COUNTS.medium
  const tableGenerators: { name: string; generator: (n: number) => Table }[] = [
    { name: "Int32", generator: generateIntTable },
    { name: "Float64", generator: generateFloatTable },
    { name: "String", generator: generateStringTable }
  ]

  console.log(`\nColumn type comparison (${rowCount.toLocaleString()} rows):`)

  for (const { name, generator } of tableGenerators) {
    const table = generator(rowCount)
    const dataBytes = estimateTableSize(table)

    const result = await runAsyncBenchmark({
      name: `Round-trip ${name}`,
      category,
      fn: async () => {
        const flightData: FlightData[] = []
        for await (const fd of encodeTableToFlightData(table)) {
          flightData.push(fd)
        }
        const decoded = await decodeFlightDataToTable(asyncIterable(flightData))
        if (decoded.numRows !== table.numRows) {
          throw new Error("row count mismatch")
        }
      },
      iterations: config.benchmarkIterations,
      warmupIterations: config.warmupIterations,
      rowCount,
      dataBytes
    })
    results.push(result)
    printResult(result)
  }

  return { name: "Round-Trip", results }
}

/**
 * Benchmark low-level IPC utilities.
 */
function benchmarkIpcUtilities(): BenchmarkSuite {
  const results: BenchmarkResult[] = []
  const category = "IPC Utilities"

  console.log(`\n${"─".repeat(60)}`)
  console.log("LOW-LEVEL IPC UTILITIES")
  console.log("─".repeat(60))

  // Create test IPC data
  const table = generateMixedTable(1000)
  const writer = new RecordBatchStreamWriter()
  writer.reset(undefined, table.schema)
  for (const batch of table.batches) {
    writer.write(batch)
  }
  writer.finish()
  const ipcBytes = writer.toUint8Array(true)

  console.log(`\nUsing ${formatBytes(ipcBytes.length)} IPC message`)

  // parseIpcMessage
  const parseResult = runSyncBenchmark({
    name: "parseIpcMessage",
    category,
    fn: () => {
      const { header, body } = parseIpcMessage(ipcBytes)
      if (header.length === 0 && body.length === 0) {
        throw new Error("unexpected")
      }
    },
    iterations: config.benchmarkIterations * 10, // More iterations for fast operation
    warmupIterations: config.warmupIterations * 10,
    dataBytes: ipcBytes.length
  })
  results.push(parseResult)
  printResult(parseResult)

  // createFlightDataFromIpc
  const createResult = runSyncBenchmark({
    name: "createFlightDataFromIpc",
    category,
    fn: () => {
      const fd = createFlightDataFromIpc(ipcBytes)
      if (fd.dataHeader.length === 0 && fd.dataBody.length === 0) {
        throw new Error("unexpected")
      }
    },
    iterations: config.benchmarkIterations * 10,
    warmupIterations: config.warmupIterations * 10,
    dataBytes: ipcBytes.length
  })
  results.push(createResult)
  printResult(createResult)

  // tableFromArrays (baseline for table creation)
  const tableCreateResult = runSyncBenchmark({
    name: "tableFromArrays (1K rows)",
    category,
    fn: () => {
      const t = generateMixedTable(1000)
      if (t.numRows === 0) {
        throw new Error("unexpected")
      }
    },
    iterations: config.benchmarkIterations,
    warmupIterations: config.warmupIterations
  })
  results.push(tableCreateResult)
  printResult(tableCreateResult)

  return { name: "IPC Utilities", results }
}

// ─────────────────────────────────────────────────────────────────────────────
// Main
// ─────────────────────────────────────────────────────────────────────────────

async function main(): Promise<void> {
  console.log("╔════════════════════════════════════════════════════════════════╗")
  console.log("║       Arrow Flight Client - Comprehensive Benchmarks          ║")
  console.log("╚════════════════════════════════════════════════════════════════╝")
  console.log()
  console.log(`Configuration:`)
  console.log(`  Warmup iterations: ${String(config.warmupIterations)}`)
  console.log(`  Benchmark iterations: ${String(config.benchmarkIterations)}`)
  console.log(`  Sizes: ${config.sizes.join(", ")}`)
  console.log(
    `  Row counts: ${config.sizes.map((s) => `${s}=${ROW_COUNTS[s].toLocaleString()}`).join(", ")}`
  )

  const suites: BenchmarkSuite[] = []

  // Run all benchmark suites
  suites.push(await benchmarkEncoding())
  suites.push(await benchmarkDecoding())
  suites.push(await benchmarkColumnTypes())
  suites.push(await benchmarkRoundTrip())
  suites.push(benchmarkIpcUtilities())

  // Print summary table
  printSummaryTable(suites)

  console.log(`\n${"═".repeat(100)}`)
  console.log("Benchmarks complete.")
}

main().catch(console.error)
