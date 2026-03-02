import { describe, expect, it, vi } from "vitest"

// Mock the ConnectRPC modules to avoid network calls
vi.mock("@connectrpc/connect", () => ({
  createClient: vi.fn(() => ({}))
}))

vi.mock("@connectrpc/connect-node", () => ({
  createGrpcTransport: vi.fn(() => ({}))
}))

// Mock @bufbuild/protobuf for message creation
vi.mock("@bufbuild/protobuf", () => ({
  create: vi.fn((_schema, init) => init),
  toBinary: vi.fn(() => new Uint8Array([1, 2, 3]))
}))

// Mock the generated proto module to avoid @bufbuild/protobuf import issues
vi.mock("../../../gen/arrow/flight/Flight_pb.js", () => ({
  FlightService: {},
  BasicAuthSchema: {}
}))

// Import after mocks are set up
const { createFlightClient } = await import("../../../client/create-flight-client.js")
const { FlightClient } = await import("../../../client/flight-client.js")

describe("createFlightClient", () => {
  it("creates a FlightClient instance", () => {
    const client = createFlightClient({ url: "https://flight.example.com:8815" })

    expect(client).toBeInstanceOf(FlightClient)
  })

  it("passes options to FlightClient", () => {
    const client = createFlightClient({
      url: "https://flight.example.com:8815",
      headers: { Authorization: "Bearer token" },
      timeoutMs: 45000
    })

    expect(client.url).toBe("https://flight.example.com:8815")
  })

  it("returns a new instance each time", () => {
    const options = { url: "https://flight.example.com:8815" }

    const client1 = createFlightClient(options)
    const client2 = createFlightClient(options)

    expect(client1).not.toBe(client2)
  })

  it("client can be closed after creation", () => {
    const client = createFlightClient({ url: "https://example.com" })

    expect(client.closed).toBe(false)
    client.close()
    expect(client.closed).toBe(true)
  })
})
