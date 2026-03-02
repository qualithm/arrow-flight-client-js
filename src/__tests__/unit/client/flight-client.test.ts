import { beforeEach, describe, expect, it, vi } from "vitest"

import { FlightConnectionError, FlightError, FlightServerError } from "../../../client/errors.js"

// Create mock implementations
const mockGetFlightInfo = vi.fn()
const mockPollFlightInfo = vi.fn()
const mockGetSchema = vi.fn()
const mockListFlights = vi.fn()
const mockListActions = vi.fn()
const mockDoAction = vi.fn()
const mockDoGet = vi.fn()

const mockCreateClient = vi.fn(() => ({
  getFlightInfo: mockGetFlightInfo,
  pollFlightInfo: mockPollFlightInfo,
  getSchema: mockGetSchema,
  listFlights: mockListFlights,
  listActions: mockListActions,
  doAction: mockDoAction,
  doGet: mockDoGet
}))

// Mock the ConnectRPC modules to avoid network calls
vi.mock("@connectrpc/connect", () => ({
  createClient: mockCreateClient
}))

vi.mock("@connectrpc/connect-node", () => ({
  createGrpcTransport: vi.fn(() => ({}))
}))

// Mock the generated proto module to avoid @bufbuild/protobuf import issues
vi.mock("../../../gen/arrow/flight/Flight_connect.js", () => ({
  FlightService: {}
}))

vi.mock("../../../gen/arrow/flight/Flight_pb.js", () => ({}))

// Import after mocks are set up
const { FlightClient } = await import("../../../client/flight-client.js")

// Helper to create async iterables for testing
// eslint-disable-next-line @typescript-eslint/require-await
async function* asyncIterable<T>(items: T[]): AsyncIterable<T> {
  for (const item of items) {
    yield item
  }
}

beforeEach(() => {
  vi.clearAllMocks()
})

describe("FlightClient", () => {
  describe("constructor", () => {
    it("creates client with minimal options", () => {
      const client = new FlightClient({ url: "https://flight.example.com:8815" })

      expect(client).toBeInstanceOf(FlightClient)
      expect(client.url).toBe("https://flight.example.com:8815")
      expect(client.closed).toBe(false)
    })

    it("creates client with all options", () => {
      const client = new FlightClient({
        url: "https://flight.example.com:8815",
        headers: { Authorization: "Bearer token" },
        timeoutMs: 60000
      })

      expect(client.url).toBe("https://flight.example.com:8815")
    })
  })

  describe("url getter", () => {
    it("returns the configured URL", () => {
      const client = new FlightClient({ url: "http://localhost:9999" })

      expect(client.url).toBe("http://localhost:9999")
    })
  })

  describe("closed getter", () => {
    it("returns false initially", () => {
      const client = new FlightClient({ url: "http://localhost:8815" })

      expect(client.closed).toBe(false)
    })

    it("returns true after close", () => {
      const client = new FlightClient({ url: "http://localhost:8815" })

      client.close()

      expect(client.closed).toBe(true)
    })
  })

  describe("close", () => {
    it("marks client as closed", () => {
      const client = new FlightClient({ url: "http://localhost:8815" })

      client.close()

      expect(client.closed).toBe(true)
    })

    it("can be called multiple times", () => {
      const client = new FlightClient({ url: "http://localhost:8815" })

      client.close()
      client.close()

      expect(client.closed).toBe(true)
    })
  })

  describe("getFlightInfo", () => {
    it("throws FlightError when client is closed", async () => {
      const client = new FlightClient({ url: "http://localhost:8815" })
      client.close()

      await expect(client.getFlightInfo({ type: "path", path: ["test"] })).rejects.toThrow(
        FlightError
      )

      await expect(client.getFlightInfo({ type: "path", path: ["test"] })).rejects.toThrow(
        "client is closed"
      )
    })

    it("returns flight info on success with path descriptor", async () => {
      const mockInfo = { flightDescriptor: { path: ["test"] } }
      mockGetFlightInfo.mockResolvedValue(mockInfo)

      const client = new FlightClient({ url: "http://localhost:8815" })
      const result = await client.getFlightInfo({ type: "path", path: ["test"] })

      expect(result).toBe(mockInfo)
      expect(mockGetFlightInfo).toHaveBeenCalledWith(
        { type: 1, path: ["test"] },
        expect.objectContaining({})
      )
    })

    it("returns flight info on success with cmd descriptor", async () => {
      const mockInfo = { flightDescriptor: { cmd: new Uint8Array([1, 2, 3]) } }
      mockGetFlightInfo.mockResolvedValue(mockInfo)

      const client = new FlightClient({ url: "http://localhost:8815" })
      const result = await client.getFlightInfo({ type: "cmd", cmd: new Uint8Array([1, 2, 3]) })

      expect(result).toBe(mockInfo)
      expect(mockGetFlightInfo).toHaveBeenCalledWith(
        { type: 2, cmd: new Uint8Array([1, 2, 3]) },
        expect.objectContaining({})
      )
    })

    it("wraps ConnectRPC errors as FlightServerError", async () => {
      const connectError = Object.assign(new Error("not found"), {
        code: "NOT_FOUND",
        rawMessage: "Flight not found"
      })
      mockGetFlightInfo.mockRejectedValue(connectError)

      const client = new FlightClient({ url: "http://localhost:8815" })

      await expect(client.getFlightInfo({ type: "path", path: ["test"] })).rejects.toThrow(
        FlightServerError
      )
    })

    it("wraps connection errors as FlightConnectionError", async () => {
      const connError = new Error("connect ECONNREFUSED 127.0.0.1:8815")
      mockGetFlightInfo.mockRejectedValue(connError)

      const client = new FlightClient({ url: "http://localhost:8815" })

      await expect(client.getFlightInfo({ type: "path", path: ["test"] })).rejects.toThrow(
        FlightConnectionError
      )
    })

    it("wraps unknown errors as FlightError", async () => {
      mockGetFlightInfo.mockRejectedValue("unknown error")

      const client = new FlightClient({ url: "http://localhost:8815" })

      await expect(client.getFlightInfo({ type: "path", path: ["test"] })).rejects.toThrow(
        FlightError
      )
    })

    it("returns FlightError unchanged", async () => {
      const flightError = new FlightError("already wrapped")
      mockGetFlightInfo.mockRejectedValue(flightError)

      const client = new FlightClient({ url: "http://localhost:8815" })

      await expect(client.getFlightInfo({ type: "path", path: ["test"] })).rejects.toBe(flightError)
    })
  })

  describe("pollFlightInfo", () => {
    it("throws FlightError when client is closed", async () => {
      const client = new FlightClient({ url: "http://localhost:8815" })
      client.close()

      await expect(
        client.pollFlightInfo({ type: "cmd", cmd: new Uint8Array([1, 2, 3]) })
      ).rejects.toThrow("client is closed")
    })

    it("returns poll info on success", async () => {
      const mockPollInfo = { info: { flightDescriptor: {} } }
      mockPollFlightInfo.mockResolvedValue(mockPollInfo)

      const client = new FlightClient({ url: "http://localhost:8815" })
      const result = await client.pollFlightInfo({ type: "path", path: ["test"] })

      expect(result).toBe(mockPollInfo)
    })
  })

  describe("getSchema", () => {
    it("throws FlightError when client is closed", async () => {
      const client = new FlightClient({ url: "http://localhost:8815" })
      client.close()

      await expect(client.getSchema({ type: "path", path: ["dataset"] })).rejects.toThrow(
        "client is closed"
      )
    })

    it("returns schema on success", async () => {
      const mockSchema = { schema: new Uint8Array([1, 2, 3]) }
      mockGetSchema.mockResolvedValue(mockSchema)

      const client = new FlightClient({ url: "http://localhost:8815" })
      const result = await client.getSchema({ type: "path", path: ["dataset"] })

      expect(result).toBe(mockSchema)
    })
  })

  describe("listFlights", () => {
    it("throws FlightError when client is closed", async () => {
      const client = new FlightClient({ url: "http://localhost:8815" })
      client.close()

      const iterable = client.listFlights()

      await expect(iterable[Symbol.asyncIterator]().next()).rejects.toThrow("client is closed")
    })

    it("yields flights on success", async () => {
      const flights = [{ flightDescriptor: { path: ["a"] } }, { flightDescriptor: { path: ["b"] } }]
      mockListFlights.mockReturnValue(asyncIterable(flights))

      const client = new FlightClient({ url: "http://localhost:8815" })
      const results: unknown[] = []

      for await (const flight of client.listFlights()) {
        results.push(flight)
      }

      expect(results).toEqual(flights)
    })

    it("passes criteria expression", async () => {
      mockListFlights.mockReturnValue(asyncIterable([]))

      const client = new FlightClient({ url: "http://localhost:8815" })
      const expression = new Uint8Array([1, 2, 3])

      for await (const _ of client.listFlights({ expression })) {
        // consume iterator
      }

      expect(mockListFlights).toHaveBeenCalledWith({ expression }, expect.objectContaining({}))
    })

    it("uses empty expression when no criteria provided", async () => {
      mockListFlights.mockReturnValue(asyncIterable([]))

      const client = new FlightClient({ url: "http://localhost:8815" })

      for await (const _ of client.listFlights()) {
        // consume iterator
      }

      expect(mockListFlights).toHaveBeenCalledWith(
        { expression: new Uint8Array() },
        expect.objectContaining({})
      )
    })
  })

  describe("listActions", () => {
    it("throws FlightError when client is closed", async () => {
      const client = new FlightClient({ url: "http://localhost:8815" })
      client.close()

      const iterable = client.listActions()

      await expect(iterable[Symbol.asyncIterator]().next()).rejects.toThrow("client is closed")
    })

    it("yields actions on success", async () => {
      const actions = [{ type: "action1" }, { type: "action2" }]
      mockListActions.mockReturnValue(asyncIterable(actions))

      const client = new FlightClient({ url: "http://localhost:8815" })
      const results: unknown[] = []

      for await (const action of client.listActions()) {
        results.push(action)
      }

      expect(results).toEqual(actions)
    })
  })

  describe("doAction", () => {
    it("throws FlightError when client is closed", async () => {
      const client = new FlightClient({ url: "http://localhost:8815" })
      client.close()

      const iterable = client.doAction({ type: "test-action" })

      await expect(iterable[Symbol.asyncIterator]().next()).rejects.toThrow("client is closed")
    })

    it("yields results on success", async () => {
      const results = [{ body: new Uint8Array([1]) }, { body: new Uint8Array([2]) }]
      mockDoAction.mockReturnValue(asyncIterable(results))

      const client = new FlightClient({ url: "http://localhost:8815" })
      const collected: unknown[] = []

      for await (const result of client.doAction({ type: "test-action" })) {
        collected.push(result)
      }

      expect(collected).toEqual(results)
    })

    it("passes action type and body", async () => {
      mockDoAction.mockReturnValue(asyncIterable([]))

      const client = new FlightClient({ url: "http://localhost:8815" })
      const body = new Uint8Array([1, 2, 3])

      for await (const _ of client.doAction({ type: "my-action", body })) {
        // consume iterator
      }

      expect(mockDoAction).toHaveBeenCalledWith(
        { type: "my-action", body },
        expect.objectContaining({})
      )
    })

    it("uses empty body when not provided", async () => {
      mockDoAction.mockReturnValue(asyncIterable([]))

      const client = new FlightClient({ url: "http://localhost:8815" })

      for await (const _ of client.doAction({ type: "no-body-action" })) {
        // consume iterator
      }

      expect(mockDoAction).toHaveBeenCalledWith(
        { type: "no-body-action", body: new Uint8Array() },
        expect.objectContaining({})
      )
    })
  })

  describe("doGet", () => {
    it("throws FlightError when client is closed", async () => {
      const client = new FlightClient({ url: "http://localhost:8815" })
      client.close()

      const iterable = client.doGet({ ticket: new Uint8Array([1, 2, 3]) })

      await expect(iterable[Symbol.asyncIterator]().next()).rejects.toThrow("client is closed")
    })

    it("yields data bodies on success", async () => {
      const dataMessages = [
        { dataBody: new Uint8Array([1, 2]) },
        { dataBody: new Uint8Array([3, 4]) }
      ]
      mockDoGet.mockReturnValue(asyncIterable(dataMessages))

      const client = new FlightClient({ url: "http://localhost:8815" })
      const collected: Uint8Array[] = []

      for await (const data of client.doGet({ ticket: new Uint8Array([5]) })) {
        collected.push(data)
      }

      expect(collected).toEqual([new Uint8Array([1, 2]), new Uint8Array([3, 4])])
    })
  })
})
