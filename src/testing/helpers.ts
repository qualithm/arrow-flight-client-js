/**
 * Table and RecordBatch creation helpers for tests.
 *
 * @packageDocumentation
 */

import { type RecordBatch, type Table, tableFromArrays } from "apache-arrow"

/**
 * Input data for creating test tables.
 * Keys are column names, values are typed arrays or regular arrays.
 */
export type TestTableData = Parameters<typeof tableFromArrays>[0]

/**
 * Create an Arrow Table from simple arrays.
 *
 * Automatically infers types from the input arrays:
 * - Int32Array → Int32
 * - Float64Array → Float64
 * - string[] → Utf8
 * - number[] → Float64
 *
 * @param data - Object mapping column names to arrays
 * @returns Arrow Table
 *
 * @example
 * ```ts
 * const table = createTestTable({
 *   id: new Int32Array([1, 2, 3]),
 *   value: new Float64Array([1.1, 2.2, 3.3]),
 *   name: ["alice", "bob", "charlie"]
 * })
 * ```
 */
export function createTestTable(data: TestTableData): Table {
  return tableFromArrays(data)
}

/**
 * Create a single RecordBatch from simple arrays.
 *
 * @param data - Object mapping column names to arrays
 * @returns Arrow RecordBatch
 *
 * @example
 * ```ts
 * const batch = createTestBatch({
 *   id: new Int32Array([1, 2, 3]),
 *   value: new Float64Array([1.1, 2.2, 3.3])
 * })
 * ```
 */
export function createTestBatch(data: TestTableData): RecordBatch {
  const table = createTestTable(data)
  // Tables created from tableFromArrays have exactly one batch
  return table.batches[0]
}

/**
 * Create a Table with standard integer test data.
 *
 * @param count - Number of rows (default: 10)
 * @returns Table with `id` (Int32) and `value` (Int32) columns
 */
export function createIntegerTable(count = 10): Table {
  const ids = new Int32Array(count)
  const values = new Int32Array(count)

  for (let i = 0; i < count; i++) {
    ids[i] = i + 1
    values[i] = (i + 1) * 10
  }

  return createTestTable({ id: ids, value: values })
}

/**
 * Create a Table with standard float test data.
 *
 * @param count - Number of rows (default: 10)
 * @returns Table with `id` (Int32) and `value` (Float64) columns
 */
export function createFloatTable(count = 10): Table {
  const ids = new Int32Array(count)
  const values = new Float64Array(count)

  for (let i = 0; i < count; i++) {
    ids[i] = i + 1
    values[i] = (i + 1) * 1.1
  }

  return createTestTable({ id: ids, value: values })
}

/**
 * Create a Table with string test data.
 *
 * @param count - Number of rows (default: 10)
 * @returns Table with `id` (Int32) and `name` (Utf8) columns
 */
export function createStringTable(count = 10): Table {
  const ids = new Int32Array(count)
  const names: string[] = []

  for (let i = 0; i < count; i++) {
    ids[i] = i + 1
    names.push(`item-${String(i + 1)}`)
  }

  return createTestTable({ id: ids, name: names })
}
