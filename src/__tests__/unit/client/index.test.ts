import { describe, expect, it, vi } from "vitest"

// Mock the ConnectRPC modules
vi.mock("@connectrpc/connect", () => ({
  createClient: vi.fn(() => ({}))
}))

vi.mock("@connectrpc/connect-node", () => ({
  createGrpcTransport: vi.fn(() => ({}))
}))

describe("client barrel exports", () => {
  it("exports all expected symbols", async () => {
    const clientModule = await import("../../../client/index.js")

    // Client exports
    expect(clientModule.createFlightClient).toBeDefined()
    expect(typeof clientModule.createFlightClient).toBe("function")
    expect(clientModule.FlightClient).toBeDefined()
    expect(typeof clientModule.FlightClient).toBe("function")

    // Error exports
    expect(clientModule.FlightError).toBeDefined()
    expect(clientModule.FlightConnectionError).toBeDefined()
    expect(clientModule.FlightAuthError).toBeDefined()
    expect(clientModule.FlightTimeoutError).toBeDefined()
    expect(clientModule.FlightServerError).toBeDefined()
    expect(clientModule.FlightCancelledError).toBeDefined()

    // IPC utilities
    expect(clientModule.decodeFlightDataStream).toBeDefined()
    expect(typeof clientModule.decodeFlightDataStream).toBe("function")
    expect(clientModule.decodeFlightDataToTable).toBeDefined()
    expect(typeof clientModule.decodeFlightDataToTable).toBe("function")
    expect(clientModule.encodeRecordBatchesToFlightData).toBeDefined()
    expect(typeof clientModule.encodeRecordBatchesToFlightData).toBe("function")
    expect(clientModule.encodeTableToFlightData).toBeDefined()
    expect(typeof clientModule.encodeTableToFlightData).toBe("function")
    expect(clientModule.getSchemaFromFlightData).toBeDefined()
    expect(typeof clientModule.getSchemaFromFlightData).toBe("function")

    // Type-related constant exports
    expect(clientModule.DEFAULT_TIMEOUT_MS).toBe(30_000)
  })
})
