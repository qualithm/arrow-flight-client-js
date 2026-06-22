/**
 * Integration tests for Arrow datatype decoding over Flight SQL.
 *
 * These guard the client's ability to decode the common Arrow datatypes a
 * Flight SQL server emits in a `SELECT` result: integers, floats, booleans and
 * — importantly — UTF-8 strings. String decoding is the most fragile path:
 * apache-arrow JS cannot decode Arrow *View* types (`Utf8View` = type id 24,
 * `BinaryView` = 23), so a server must emit canonical `Utf8`/`Binary` for this
 * client to read string columns. This test asserts the string column decodes
 * to a JS string and carries the canonical `Utf8` type id (5), never a View id.
 *
 * Runs against any Flight SQL server exposing the standard `test.*` fixtures
 * (see `config.ts`). Skips gracefully when no server is reachable.
 */
import { afterAll, beforeAll, describe, expect, it } from "vitest"

import { createFlightSqlClient, type FlightSqlClient } from "../../client"
import { config, isFlightAvailable } from "./config"

describe("Datatype Decoding Integration", () => {
  let client: FlightSqlClient
  let available: boolean

  beforeAll(async () => {
    available = await isFlightAvailable()
    if (!available) {
      return
    }

    const auth =
      config.bearerToken !== undefined
        ? { type: "bearer" as const, token: config.bearerToken }
        : { type: "basic" as const, credentials: config.credentials.admin }

    client = createFlightSqlClient({ url: config.url, auth })

    if (config.bearerToken === undefined) {
      await client.authenticate()
    }
  })

  afterAll(() => {
    if (available) {
      client.close()
    }
  })

  it("decodes a UTF-8 string column to a JS string", async () => {
    if (!available) {
      return
    }

    const table = await client.query(`SELECT id, name FROM ${config.tables.strings}`)

    expect(table.numRows).toBeGreaterThan(0)

    const field = table.schema.fields.find((f) => f.name === "name")
    expect(field).toBeDefined()
    // Utf8 (Arrow type id 5), never Utf8View (24) — the latter is undecodable
    // by apache-arrow JS.
    expect(field?.typeId).toBe(5)

    const rows = table.toArray()
    expect(typeof rows[0].name).toBe("string")
  })

  it("decodes the spread of common Arrow datatypes", async () => {
    if (!available) {
      return
    }

    const table = await client.query(`SELECT * FROM ${config.tables.allTypes}`)

    expect(table.numRows).toBeGreaterThan(0)

    const names = table.schema.fields.map((f) => f.name)
    expect(names).toContain("col_int64")
    expect(names).toContain("col_float64")
    expect(names).toContain("col_boolean")
    expect(names).toContain("col_utf8")

    const utf8 = table.schema.fields.find((f) => f.name === "col_utf8")
    // Utf8 (5) and Bool (6) are stable Arrow type ids.
    expect(utf8?.typeId).toBe(5)
    const boolean = table.schema.fields.find((f) => f.name === "col_boolean")
    expect(boolean?.typeId).toBe(6)

    const row = table.toArray()[0]
    expect(typeof row.col_utf8).toBe("string")
    expect(typeof row.col_boolean).toBe("boolean")
    expect(typeof row.col_float64).toBe("number")
    // 64-bit integers decode to BigInt in apache-arrow JS.
    expect(["bigint", "number"]).toContain(typeof row.col_int64)
  })
})
