import { beforeEach, describe, expect, it, vi } from "vitest"

import {
  FlightAuthError,
  FlightConnectionError,
  FlightError,
  FlightServerError
} from "../../../client/errors.js"

// Create mock implementations
const mockGetFlightInfo = vi.fn()
const mockPollFlightInfo = vi.fn()
const mockGetSchema = vi.fn()
const mockListFlights = vi.fn()
const mockListActions = vi.fn()
const mockDoAction = vi.fn()
const mockDoGet = vi.fn()
const mockDoPut = vi.fn()
const mockHandshake = vi.fn()

const mockCreateClient = vi.fn(() => ({
  getFlightInfo: mockGetFlightInfo,
  pollFlightInfo: mockPollFlightInfo,
  getSchema: mockGetSchema,
  listFlights: mockListFlights,
  listActions: mockListActions,
  doAction: mockDoAction,
  doGet: mockDoGet,
  doPut: mockDoPut,
  handshake: mockHandshake
}))

const mockCreateGrpcTransport = vi.fn(() => ({}))

// Mock the ConnectRPC modules to avoid network calls. Everything except
// `createClient` re-exports the real module, so the doubles below can use the
// real ConnectError/Code — a fabricated `code: "UNAUTHENTICATED"` string never
// matches the numeric enum a real rejection carries, which is how the re-auth
// matcher regressed unnoticed.
vi.mock("@connectrpc/connect", async (importOriginal) => {
  const actual = await importOriginal<Record<string, unknown>>()
  return { ...actual, createClient: mockCreateClient }
})

vi.mock("@connectrpc/connect-node", () => ({
  createGrpcTransport: mockCreateGrpcTransport
}))

// Mock @bufbuild/protobuf for message creation
vi.mock("@bufbuild/protobuf", () => ({
  create: vi.fn((_schema, init) => init),
  toBinary: vi.fn(() => new Uint8Array([1, 2, 3]))
}))

// Mock the generated proto module to avoid @bufbuild/protobuf import issues
vi.mock("../../../gen/arrow/flight/Flight_pb.js", () => ({
  FlightService: {},
  BasicAuthSchema: {},
  FlightDescriptorSchema: {}
}))

// Import after mocks are set up. Code/ConnectError come from the mocked module
// (which re-exports the real ones); a static import would hoist above the mock
// factories and hit their TDZ.
const { FlightClient } = await import("../../../client/flight-client.js")
const { Code, ConnectError } = await import("@connectrpc/connect")

// Helper to create async iterables for testing
async function* asyncIterable<T>(items: T[]): AsyncIterable<T> {
  for (const item of items) {
    yield await Promise.resolve(item)
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

    it("wraps ConnectRPC errors as FlightServerError with the wire code name", async () => {
      const connectError = new ConnectError("not found", Code.NotFound)
      mockGetFlightInfo.mockRejectedValue(connectError)

      const client = new FlightClient({ url: "http://localhost:8815" })

      const rejection = client.getFlightInfo({ type: "path", path: ["test"] })
      await expect(rejection).rejects.toThrow(FlightServerError)
      await expect(rejection).rejects.toMatchObject({ code: "not_found" })
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

    it("wraps errors as FlightError", async () => {
      mockPollFlightInfo.mockRejectedValue(new Error("network error"))

      const client = new FlightClient({ url: "http://localhost:8815" })

      await expect(client.pollFlightInfo({ type: "path", path: ["test"] })).rejects.toThrow(
        FlightError
      )
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

    it("wraps errors as FlightError", async () => {
      mockGetSchema.mockRejectedValue(new Error("network error"))

      const client = new FlightClient({ url: "http://localhost:8815" })

      await expect(client.getSchema({ type: "path", path: ["dataset"] })).rejects.toThrow(
        FlightError
      )
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

    it("wraps errors as FlightError", async () => {
      mockListFlights.mockImplementation(() => {
        throw new Error("network error")
      })

      const client = new FlightClient({ url: "http://localhost:8815" })

      await expect(client.listFlights()[Symbol.asyncIterator]().next()).rejects.toThrow(FlightError)
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

    it("wraps errors as FlightError", async () => {
      mockListActions.mockImplementation(() => {
        throw new Error("network error")
      })

      const client = new FlightClient({ url: "http://localhost:8815" })

      await expect(client.listActions()[Symbol.asyncIterator]().next()).rejects.toThrow(FlightError)
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

    it("wraps errors as FlightError", async () => {
      mockDoAction.mockImplementation(() => {
        throw new Error("network error")
      })

      const client = new FlightClient({ url: "http://localhost:8815" })

      await expect(
        client.doAction({ type: "test" })[Symbol.asyncIterator]().next()
      ).rejects.toThrow(FlightError)
    })
  })

  describe("doGet", () => {
    it("throws FlightError when client is closed", async () => {
      const client = new FlightClient({ url: "http://localhost:8815" })
      client.close()

      const iterable = client.doGet({ ticket: new Uint8Array([1, 2, 3]) })

      await expect(iterable[Symbol.asyncIterator]().next()).rejects.toThrow("client is closed")
    })

    it("yields FlightData messages on success", async () => {
      const dataMessages = [
        {
          $typeName: "arrow.flight.protocol.FlightData",
          dataHeader: new Uint8Array([0]),
          dataBody: new Uint8Array([1, 2]),
          appMetadata: new Uint8Array()
        },
        {
          $typeName: "arrow.flight.protocol.FlightData",
          dataHeader: new Uint8Array([0]),
          dataBody: new Uint8Array([3, 4]),
          appMetadata: new Uint8Array()
        }
      ]
      mockDoGet.mockReturnValue(asyncIterable(dataMessages))

      const client = new FlightClient({ url: "http://localhost:8815" })
      const collected: unknown[] = []

      for await (const data of client.doGet({ ticket: new Uint8Array([5]) })) {
        collected.push(data)
      }

      expect(collected).toHaveLength(2)
      expect(collected[0]).toMatchObject({ dataBody: new Uint8Array([1, 2]) })
      expect(collected[1]).toMatchObject({ dataBody: new Uint8Array([3, 4]) })
    })

    it("wraps errors as FlightError", async () => {
      mockDoGet.mockImplementation(() => {
        throw new Error("network error")
      })

      const client = new FlightClient({ url: "http://localhost:8815" })
      const iterable = client.doGet({ ticket: new Uint8Array([1]) })

      await expect(iterable[Symbol.asyncIterator]().next()).rejects.toThrow(FlightError)
    })
  })

  describe("doPut", () => {
    it("throws FlightError when client is closed", async () => {
      const client = new FlightClient({ url: "http://localhost:8815" })
      client.close()

      const data = asyncIterable([
        {
          $typeName: "arrow.flight.protocol.FlightData" as const,
          dataHeader: new Uint8Array(),
          dataBody: new Uint8Array([1, 2, 3]),
          appMetadata: new Uint8Array(),
          flightDescriptor: undefined
        }
      ])

      const iterable = client.doPut(data)

      await expect(iterable[Symbol.asyncIterator]().next()).rejects.toThrow("client is closed")
    })

    it("yields PutResult messages on success", async () => {
      const putResults = [
        { appMetadata: new Uint8Array([1]) },
        { appMetadata: new Uint8Array([2]) }
      ]
      mockDoPut.mockReturnValue(asyncIterable(putResults))

      const client = new FlightClient({ url: "http://localhost:8815" })

      const data = asyncIterable([
        {
          $typeName: "arrow.flight.protocol.FlightData" as const,
          dataHeader: new Uint8Array(),
          dataBody: new Uint8Array([1, 2, 3]),
          appMetadata: new Uint8Array(),
          flightDescriptor: undefined
        }
      ])

      const collected: unknown[] = []
      for await (const result of client.doPut(data)) {
        collected.push(result)
      }

      expect(collected).toHaveLength(2)
      expect(collected[0]).toMatchObject({ appMetadata: new Uint8Array([1]) })
      expect(collected[1]).toMatchObject({ appMetadata: new Uint8Array([2]) })
    })

    it("wraps errors as FlightError", async () => {
      mockDoPut.mockImplementation(() => {
        throw new Error("upload failed")
      })

      const client = new FlightClient({ url: "http://localhost:8815" })

      const data = asyncIterable([
        {
          $typeName: "arrow.flight.protocol.FlightData" as const,
          dataHeader: new Uint8Array(),
          dataBody: new Uint8Array([1, 2, 3]),
          appMetadata: new Uint8Array(),
          flightDescriptor: undefined
        }
      ])

      const iterable = client.doPut(data)

      await expect(iterable[Symbol.asyncIterator]().next()).rejects.toThrow(FlightError)
    })
  })

  describe("authenticated getter", () => {
    it("returns false initially", () => {
      const client = new FlightClient({ url: "http://localhost:8815" })

      expect(client.authenticated).toBe(false)
    })
  })

  describe("handshake", () => {
    it("throws FlightError when client is closed", async () => {
      const client = new FlightClient({
        url: "http://localhost:8815",
        auth: { type: "basic", credentials: { username: "user", password: "pass" } }
      })
      client.close()

      await expect(client.handshake()).rejects.toThrow("client is closed")
    })

    it("throws FlightError when no payload and no basic auth configured", async () => {
      const client = new FlightClient({ url: "http://localhost:8815" })

      await expect(client.handshake()).rejects.toThrow(
        "no handshake payload provided and no basic auth credentials configured"
      )
    })

    it("performs handshake with basic auth credentials", async () => {
      const tokenBytes = new TextEncoder().encode("test-token")
      mockHandshake.mockReturnValue(asyncIterable([{ protocolVersion: 0n, payload: tokenBytes }]))

      const client = new FlightClient({
        url: "http://localhost:8815",
        auth: { type: "basic", credentials: { username: "user", password: "pass" } }
      })

      const token = await client.handshake()

      expect(token).toBe("test-token")
      expect(client.authenticated).toBe(true)
      expect(mockHandshake).toHaveBeenCalled()
    })

    it("performs handshake with custom payload", async () => {
      const tokenBytes = new TextEncoder().encode("custom-token")
      mockHandshake.mockReturnValue(asyncIterable([{ protocolVersion: 0n, payload: tokenBytes }]))

      const client = new FlightClient({ url: "http://localhost:8815" })
      const customPayload = new Uint8Array([1, 2, 3, 4])

      const token = await client.handshake(customPayload)

      expect(token).toBe("custom-token")
      expect(client.authenticated).toBe(true)
    })

    it("throws FlightAuthError when no response from server", async () => {
      mockHandshake.mockReturnValue(asyncIterable([]))

      const client = new FlightClient({
        url: "http://localhost:8815",
        auth: { type: "basic", credentials: { username: "user", password: "pass" } }
      })

      await expect(client.handshake()).rejects.toThrow(FlightAuthError)
      await expect(client.handshake()).rejects.toThrow("handshake failed: no response from server")
    })

    it("wraps errors as FlightError", async () => {
      mockHandshake.mockImplementation(() => {
        throw new Error("network error")
      })

      const client = new FlightClient({
        url: "http://localhost:8815",
        auth: { type: "basic", credentials: { username: "user", password: "pass" } }
      })

      await expect(client.handshake()).rejects.toThrow(FlightError)
    })
  })

  describe("authenticate", () => {
    it("throws FlightError when client is closed", async () => {
      const client = new FlightClient({ url: "http://localhost:8815" })
      client.close()

      await expect(client.authenticate()).rejects.toThrow("client is closed")
    })

    it("calls handshake for basic auth", async () => {
      const tokenBytes = new TextEncoder().encode("auth-token")
      mockHandshake.mockReturnValue(asyncIterable([{ protocolVersion: 0n, payload: tokenBytes }]))

      const client = new FlightClient({
        url: "http://localhost:8815",
        auth: { type: "basic", credentials: { username: "user", password: "pass" } }
      })

      const token = await client.authenticate()

      expect(token).toBe("auth-token")
      expect(client.authenticated).toBe(true)
    })

    it("returns token and sets authenticated for bearer auth", async () => {
      const client = new FlightClient({
        url: "http://localhost:8815",
        auth: { type: "bearer", token: "my-bearer-token" }
      })

      const token = await client.authenticate()

      expect(token).toBe("my-bearer-token")
      expect(client.authenticated).toBe(true)
    })

    it("returns undefined when no auth configured", async () => {
      const client = new FlightClient({ url: "http://localhost:8815" })

      const token = await client.authenticate()

      expect(token).toBeUndefined()
    })
  })

  describe("authentication headers", () => {
    it("includes auth token in requests after handshake", async () => {
      const tokenBytes = new TextEncoder().encode("session-token")
      mockHandshake.mockReturnValue(asyncIterable([{ protocolVersion: 0n, payload: tokenBytes }]))
      mockGetFlightInfo.mockResolvedValue({ flightDescriptor: {} })

      const client = new FlightClient({
        url: "http://localhost:8815",
        auth: { type: "basic", credentials: { username: "user", password: "pass" } }
      })

      await client.handshake()
      await client.getFlightInfo({ type: "path", path: ["test"] })

      expect(mockGetFlightInfo).toHaveBeenCalledWith(
        expect.anything(),
        expect.objectContaining({
          headers: expect.objectContaining({
            Authorization: "Bearer session-token"
          })
        })
      )
    })

    it("includes bearer token in requests without handshake", async () => {
      mockGetFlightInfo.mockResolvedValue({ flightDescriptor: {} })

      const client = new FlightClient({
        url: "http://localhost:8815",
        auth: { type: "bearer", token: "my-token" }
      })

      await client.getFlightInfo({ type: "path", path: ["test"] })

      expect(mockGetFlightInfo).toHaveBeenCalledWith(
        expect.anything(),
        expect.objectContaining({
          headers: expect.objectContaining({
            Authorization: "Bearer my-token"
          })
        })
      )
    })

    it("preserves custom headers alongside auth", async () => {
      mockGetFlightInfo.mockResolvedValue({ flightDescriptor: {} })

      const client = new FlightClient({
        url: "http://localhost:8815",
        headers: { "X-Custom": "value" },
        auth: { type: "bearer", token: "my-token" }
      })

      await client.getFlightInfo({ type: "path", path: ["test"] })

      expect(mockGetFlightInfo).toHaveBeenCalledWith(
        expect.anything(),
        expect.objectContaining({
          headers: expect.objectContaining({
            "X-Custom": "value",
            Authorization: "Bearer my-token"
          })
        })
      )
    })
  })

  describe("error handling for auth errors", () => {
    it("wraps UNAUTHENTICATED errors as FlightAuthError", async () => {
      const authError = new ConnectError("invalid or expired token", Code.Unauthenticated)
      mockGetFlightInfo.mockRejectedValue(authError)

      const client = new FlightClient({ url: "http://localhost:8815" })

      await expect(client.getFlightInfo({ type: "path", path: ["test"] })).rejects.toThrow(
        FlightAuthError
      )
    })

    it("wraps PERMISSION_DENIED errors as FlightAuthError", async () => {
      const authError = new ConnectError("permission denied", Code.PermissionDenied)
      mockGetFlightInfo.mockRejectedValue(authError)

      const client = new FlightClient({ url: "http://localhost:8815" })

      await expect(client.getFlightInfo({ type: "path", path: ["test"] })).rejects.toThrow(
        FlightAuthError
      )
    })
  })

  describe("TLS configuration", () => {
    it("passes TLS cert and key to transport", () => {
      new FlightClient({
        url: "https://flight.example.com",
        tls: {
          cert: "cert-data",
          key: "key-data"
        }
      })

      expect(mockCreateGrpcTransport).toHaveBeenCalledWith(
        expect.objectContaining({
          nodeOptions: expect.objectContaining({
            cert: "cert-data",
            key: "key-data"
          })
        })
      )
    })

    it("passes TLS ca to transport", () => {
      new FlightClient({
        url: "https://flight.example.com",
        tls: {
          ca: "ca-data"
        }
      })

      expect(mockCreateGrpcTransport).toHaveBeenCalledWith(
        expect.objectContaining({
          nodeOptions: expect.objectContaining({
            ca: "ca-data"
          })
        })
      )
    })

    it("passes TLS passphrase to transport", () => {
      new FlightClient({
        url: "https://flight.example.com",
        tls: {
          passphrase: "secret"
        }
      })

      expect(mockCreateGrpcTransport).toHaveBeenCalledWith(
        expect.objectContaining({
          nodeOptions: expect.objectContaining({
            passphrase: "secret"
          })
        })
      )
    })

    it("passes TLS rejectUnauthorized to transport", () => {
      new FlightClient({
        url: "https://flight.example.com",
        tls: {
          rejectUnauthorized: false
        }
      })

      expect(mockCreateGrpcTransport).toHaveBeenCalledWith(
        expect.objectContaining({
          nodeOptions: expect.objectContaining({
            rejectUnauthorized: false
          })
        })
      )
    })

    it("ignores empty passphrase", () => {
      new FlightClient({
        url: "https://flight.example.com",
        tls: {
          passphrase: ""
        }
      })

      expect(mockCreateGrpcTransport).toHaveBeenCalledWith(
        expect.objectContaining({
          nodeOptions: expect.not.objectContaining({
            passphrase: ""
          })
        })
      )
    })

    it("merges nodeOptions with TLS options", () => {
      new FlightClient({
        url: "https://flight.example.com",
        nodeOptions: { timeout: 5000 },
        tls: {
          cert: "cert-data"
        }
      })

      expect(mockCreateGrpcTransport).toHaveBeenCalledWith(
        expect.objectContaining({
          nodeOptions: expect.objectContaining({
            timeout: 5000,
            cert: "cert-data"
          })
        })
      )
    })
  })

  describe("authProvider", () => {
    // The real rejection shape: ConnectError carries the numeric Code enum.
    const unauthenticated = (): InstanceType<typeof ConnectError> =>
      new ConnectError("token expired", Code.Unauthenticated)

    const authHeaderOf = (mock: typeof mockGetFlightInfo, call: number): string | undefined =>
      (mock.mock.calls[call]?.[1] as { headers: Record<string, string> } | undefined)?.headers
        .Authorization

    async function* failAfterFirst<T>(items: T[], error: Error): AsyncIterable<T> {
      for (const item of items) {
        yield await Promise.resolve(item)
      }
      throw error
    }

    it("resolves credentials before the first request", async () => {
      const provider = vi.fn(() => ({ type: "bearer" as const, token: "first-token" }))
      mockGetFlightInfo.mockResolvedValue({ flightDescriptor: {} })

      const client = new FlightClient({ url: "http://localhost:8815", authProvider: provider })
      await client.getFlightInfo({ type: "path", path: ["t"] })

      expect(provider).toHaveBeenCalledTimes(1)
      expect(authHeaderOf(mockGetFlightInfo, 0)).toBe("Bearer first-token")
    })

    it("does not consult the provider again while the credential holds", async () => {
      const provider = vi.fn(() => ({ type: "bearer" as const, token: "cached" }))
      mockGetFlightInfo.mockResolvedValue({ flightDescriptor: {} })

      const client = new FlightClient({ url: "http://localhost:8815", authProvider: provider })
      await client.getFlightInfo({ type: "path", path: ["a"] })
      await client.getFlightInfo({ type: "path", path: ["b"] })
      await client.getFlightInfo({ type: "path", path: ["c"] })

      expect(provider).toHaveBeenCalledTimes(1)
      expect(mockGetFlightInfo).toHaveBeenCalledTimes(3)
    })

    it("adopts a rotated credential on an existing client", async () => {
      const provider = vi
        .fn()
        .mockReturnValueOnce({ type: "bearer", token: "stale" })
        .mockReturnValueOnce({ type: "bearer", token: "rotated" })
      mockGetFlightInfo
        .mockRejectedValueOnce(unauthenticated())
        .mockResolvedValue({ flightDescriptor: {} })

      const client = new FlightClient({ url: "http://localhost:8815", authProvider: provider })
      const info = await client.getFlightInfo({ type: "path", path: ["t"] })

      expect(info).toEqual({ flightDescriptor: {} })
      expect(mockGetFlightInfo).toHaveBeenCalledTimes(2)
      expect(authHeaderOf(mockGetFlightInfo, 0)).toBe("Bearer stale")
      expect(authHeaderOf(mockGetFlightInfo, 1)).toBe("Bearer rotated")
    })

    it("re-runs the handshake for basic credentials", async () => {
      const provider = vi.fn(() => ({
        type: "basic" as const,
        credentials: { username: "u", password: "p" }
      }))
      mockHandshake
        .mockReturnValueOnce(
          asyncIterable([{ protocolVersion: 0n, payload: new TextEncoder().encode("session-1") }])
        )
        .mockReturnValueOnce(
          asyncIterable([{ protocolVersion: 0n, payload: new TextEncoder().encode("session-2") }])
        )
      mockGetFlightInfo
        .mockRejectedValueOnce(unauthenticated())
        .mockResolvedValue({ flightDescriptor: {} })

      const client = new FlightClient({ url: "http://localhost:8815", authProvider: provider })
      await client.getFlightInfo({ type: "path", path: ["t"] })

      expect(mockHandshake).toHaveBeenCalledTimes(2)
      expect(authHeaderOf(mockGetFlightInfo, 0)).toBe("Bearer session-1")
      expect(authHeaderOf(mockGetFlightInfo, 1)).toBe("Bearer session-2")
    })

    it("retries exactly once, then surfaces the rejection", async () => {
      const provider = vi.fn(() => ({ type: "bearer" as const, token: "no-good" }))
      mockGetFlightInfo.mockRejectedValue(unauthenticated())

      const client = new FlightClient({ url: "http://localhost:8815", authProvider: provider })

      await expect(client.getFlightInfo({ type: "path", path: ["t"] })).rejects.toThrow(
        FlightAuthError
      )
      expect(mockGetFlightInfo).toHaveBeenCalledTimes(2)
      expect(provider).toHaveBeenCalledTimes(2)
    })

    it("does not retry errors that are not auth failures", async () => {
      const provider = vi.fn(() => ({ type: "bearer" as const, token: "fine" }))
      mockGetFlightInfo.mockRejectedValue(new ConnectError("boom", Code.Unavailable))

      const client = new FlightClient({ url: "http://localhost:8815", authProvider: provider })

      await expect(client.getFlightInfo({ type: "path", path: ["t"] })).rejects.toThrow(
        FlightServerError
      )
      expect(mockGetFlightInfo).toHaveBeenCalledTimes(1)
    })

    it("does not retry when no provider is configured", async () => {
      mockGetFlightInfo.mockRejectedValue(unauthenticated())

      const client = new FlightClient({
        url: "http://localhost:8815",
        auth: { type: "bearer", token: "static" }
      })

      await expect(client.getFlightInfo({ type: "path", path: ["t"] })).rejects.toThrow(
        FlightAuthError
      )
      expect(mockGetFlightInfo).toHaveBeenCalledTimes(1)
    })

    it("coalesces concurrent rejections into a single refresh", async () => {
      const provider = vi.fn(() => ({ type: "bearer" as const, token: "shared" }))
      mockGetFlightInfo
        .mockRejectedValueOnce(unauthenticated())
        .mockRejectedValueOnce(unauthenticated())
        .mockResolvedValue({ flightDescriptor: {} })

      const client = new FlightClient({ url: "http://localhost:8815", authProvider: provider })
      await Promise.all([
        client.getFlightInfo({ type: "path", path: ["a"] }),
        client.getFlightInfo({ type: "path", path: ["b"] })
      ])

      // One eager resolve plus one shared refresh, not one refresh per rejection
      expect(provider).toHaveBeenCalledTimes(2)
    })

    it("retries a stream rejected before its first message", async () => {
      const provider = vi.fn(() => ({ type: "bearer" as const, token: "t" }))
      mockListFlights
        .mockImplementationOnce(() => {
          throw unauthenticated()
        })
        .mockImplementation(() => asyncIterable([{ flightDescriptor: { path: ["a"] } }]))

      const client = new FlightClient({ url: "http://localhost:8815", authProvider: provider })
      const seen: unknown[] = []
      for await (const info of client.listFlights()) {
        seen.push(info)
      }

      expect(seen).toHaveLength(1)
      expect(mockListFlights).toHaveBeenCalledTimes(2)
    })

    it("does not retry a stream that has already delivered messages", async () => {
      const provider = vi.fn(() => ({ type: "bearer" as const, token: "t" }))
      mockListFlights.mockImplementation(() =>
        failAfterFirst([{ flightDescriptor: { path: ["a"] } }], unauthenticated())
      )

      const client = new FlightClient({ url: "http://localhost:8815", authProvider: provider })
      const seen: unknown[] = []

      await expect(async () => {
        for await (const info of client.listFlights()) {
          seen.push(info)
        }
      }).rejects.toThrow(FlightAuthError)

      expect(seen).toHaveLength(1)
      expect(mockListFlights).toHaveBeenCalledTimes(1)
    })

    it("does not retry doPut, whose input stream cannot be replayed", async () => {
      const provider = vi.fn(() => ({ type: "bearer" as const, token: "t" }))
      mockDoPut.mockImplementation(() => {
        throw unauthenticated()
      })

      const client = new FlightClient({ url: "http://localhost:8815", authProvider: provider })

      await expect(async () => {
        for await (const _ of client.doPut(asyncIterable([]))) {
          // drain
        }
      }).rejects.toThrow(FlightAuthError)

      expect(mockDoPut).toHaveBeenCalledTimes(1)
    })

    it("re-resolves through the provider when authenticate is called", async () => {
      const provider = vi
        .fn()
        .mockReturnValueOnce({ type: "bearer", token: "old" })
        .mockReturnValueOnce({ type: "bearer", token: "new" })

      const client = new FlightClient({ url: "http://localhost:8815", authProvider: provider })

      expect(await client.authenticate()).toBe("old")
      expect(await client.authenticate()).toBe("new")
      expect(client.authenticated).toBe(true)
    })

    it("takes precedence over static auth", async () => {
      const provider = vi.fn(() => ({ type: "bearer" as const, token: "from-provider" }))
      mockGetFlightInfo.mockResolvedValue({ flightDescriptor: {} })

      const client = new FlightClient({
        url: "http://localhost:8815",
        auth: { type: "bearer", token: "from-static" },
        authProvider: provider
      })
      await client.getFlightInfo({ type: "path", path: ["t"] })

      expect(authHeaderOf(mockGetFlightInfo, 0)).toBe("Bearer from-provider")
    })

    it("sends no credentials when the provider resolves to none", async () => {
      const provider = vi.fn(() => ({ type: "none" as const }))
      mockGetFlightInfo.mockResolvedValue({ flightDescriptor: {} })

      const client = new FlightClient({ url: "http://localhost:8815", authProvider: provider })

      expect(await client.authenticate()).toBeUndefined()
      await client.getFlightInfo({ type: "path", path: ["t"] })

      expect(authHeaderOf(mockGetFlightInfo, 0)).toBeUndefined()
      expect(client.authenticated).toBe(false)
    })

    it("surfaces a stream rejection that survives the retry", async () => {
      const provider = vi.fn(() => ({ type: "bearer" as const, token: "t" }))
      mockListFlights.mockImplementation(() => {
        throw unauthenticated()
      })

      const client = new FlightClient({ url: "http://localhost:8815", authProvider: provider })

      await expect(async () => {
        for await (const _ of client.listFlights()) {
          // drain
        }
      }).rejects.toThrow(FlightAuthError)

      expect(mockListFlights).toHaveBeenCalledTimes(2)
    })
  })
})
