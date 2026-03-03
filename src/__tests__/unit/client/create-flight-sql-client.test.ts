import { describe, expect, it } from "vitest"

import { createFlightSqlClient } from "../../../client/create-flight-sql-client.js"
import { FlightSqlClient } from "../../../client/flight-sql-client.js"

describe("createFlightSqlClient", () => {
  it("creates a FlightSqlClient instance", () => {
    const client = createFlightSqlClient({
      url: "https://flight.example.com:8815"
    })

    expect(client).toBeInstanceOf(FlightSqlClient)
  })

  it("passes options and creates client with correct url", () => {
    const options = {
      url: "https://flight.example.com:8815",
      headers: { "X-Custom": "value" },
      timeoutMs: 60000
    }

    const client = createFlightSqlClient(options)

    expect(client).toBeInstanceOf(FlightSqlClient)
    expect(client.url).toBe("https://flight.example.com:8815")
  })
})
