import { create, toBinary } from "@bufbuild/protobuf"
import { tableFromArrays } from "apache-arrow"
import { describe, expect, it, vi } from "vitest"

import { FlightError } from "../../../client/errors.js"
import type { FlightClient } from "../../../client/flight-client.js"
import { FlightSqlClient } from "../../../client/flight-sql-client.js"
import { encodeTableToFlightData } from "../../../client/ipc.js"
import type { FlightData, FlightInfo } from "../../../gen/arrow/flight/Flight_pb.js"
import {
  ActionBeginTransactionResultSchema,
  ActionCreatePreparedStatementResultSchema,
  DoPutUpdateResultSchema
} from "../../../gen/arrow/flight/FlightSql_pb.js"

// Helper to create async iterables for testing
// eslint-disable-next-line @typescript-eslint/require-await
async function* asyncIterable<T>(items: T[]): AsyncIterable<T> {
  for (const item of items) {
    yield item
  }
}

// Helper to create a test table
// eslint-disable-next-line @typescript-eslint/explicit-function-return-type
function createTestTable() {
  return tableFromArrays({
    id: new Int32Array([1, 2, 3]),
    name: ["Alice", "Bob", "Charlie"]
  })
}

// Helper to create FlightData from a table for mocking
async function createFlightDataArray(): Promise<FlightData[]> {
  const table = createTestTable()
  const flightData: FlightData[] = []
  for await (const data of encodeTableToFlightData(table)) {
    flightData.push(data)
  }
  return flightData
}

// Helper to create a mock FlightInfo with ticket
function createMockFlightInfo(ticketBytes = new Uint8Array([1, 2, 3])): FlightInfo {
  return {
    endpoint: [{ ticket: { ticket: ticketBytes }, location: [] }],
    flightDescriptor: undefined,
    schema: new Uint8Array(),
    totalBytes: 0n,
    totalRecords: 0n,
    ordered: false,
    appMetadata: new Uint8Array()
  } as unknown as FlightInfo
}

// Helper to create a mock prepared statement response
// eslint-disable-next-line @typescript-eslint/explicit-function-return-type
function createMockPreparedStatementResponse() {
  const msg = create(ActionCreatePreparedStatementResultSchema, {
    preparedStatementHandle: new Uint8Array([1, 2, 3, 4]),
    datasetSchema: new Uint8Array(),
    parameterSchema: new Uint8Array()
  })
  const result = toBinary(ActionCreatePreparedStatementResultSchema, msg)
  return { body: result }
}

// Helper to create a mock update result response
// eslint-disable-next-line @typescript-eslint/explicit-function-return-type
function createMockUpdateResult(recordCount: bigint) {
  const msg = create(DoPutUpdateResultSchema, { recordCount })
  const result = toBinary(DoPutUpdateResultSchema, msg)
  return { appMetadata: result }
}

// Helper to create a mock transaction response
// eslint-disable-next-line @typescript-eslint/explicit-function-return-type
function createMockTransactionResponse() {
  const msg = create(ActionBeginTransactionResultSchema, {
    transactionId: new Uint8Array([10, 20, 30, 40])
  })
  const result = toBinary(ActionBeginTransactionResultSchema, msg)
  return { body: result }
}

// Create mock FlightClient factory
function createMockFlightClient(overrides: Partial<FlightClient> = {}): FlightClient {
  return {
    url: "https://flight.example.com:8815",
    closed: false,
    authenticated: false,
    getFlightInfo: vi.fn(),
    pollFlightInfo: vi.fn(),
    getSchema: vi.fn(),
    listFlights: vi.fn(),
    listActions: vi.fn(),
    doAction: vi.fn(),
    doGet: vi.fn(),
    doPut: vi.fn(),
    authenticate: vi.fn(),
    handshake: vi.fn(),
    close: vi.fn(),
    ...overrides
  } as unknown as FlightClient
}

describe("FlightSqlClient", () => {
  describe("constructor", () => {
    it("creates client with FlightClient instance", () => {
      const mockFlight = createMockFlightClient()
      const client = new FlightSqlClient(mockFlight)

      expect(client).toBeInstanceOf(FlightSqlClient)
      expect(client.flight).toBe(mockFlight)
    })

    it("creates client with options (creates internal FlightClient)", () => {
      // This test verifies the options path still works
      const client = new FlightSqlClient({ url: "https://flight.example.com:8815" })
      expect(client).toBeInstanceOf(FlightSqlClient)
      expect(client.url).toBe("https://flight.example.com:8815")
    })
  })

  describe("flight getter", () => {
    it("returns the underlying FlightClient", () => {
      const mockFlight = createMockFlightClient()
      const client = new FlightSqlClient(mockFlight)

      expect(client.flight).toBe(mockFlight)
    })
  })

  describe("url getter", () => {
    it("returns the configured URL", () => {
      const mockFlight = createMockFlightClient({ url: "https://flight.example.com:8815" })
      const client = new FlightSqlClient(mockFlight)

      expect(client.url).toBe("https://flight.example.com:8815")
    })
  })

  describe("closed getter", () => {
    it("returns false initially", () => {
      const mockFlight = createMockFlightClient({ closed: false })
      const client = new FlightSqlClient(mockFlight)

      expect(client.closed).toBe(false)
    })

    it("returns true when underlying client is closed", () => {
      const mockFlight = createMockFlightClient({ closed: true })
      const client = new FlightSqlClient(mockFlight)

      expect(client.closed).toBe(true)
    })
  })

  describe("close", () => {
    it("delegates to underlying FlightClient", () => {
      const mockClose = vi.fn()
      const mockFlight = createMockFlightClient({ close: mockClose })
      const client = new FlightSqlClient(mockFlight)

      client.close()

      expect(mockClose).toHaveBeenCalled()
    })
  })

  describe("authenticate", () => {
    it("delegates to underlying FlightClient", async () => {
      const mockAuthenticate = vi.fn().mockResolvedValue("token123")
      const mockFlight = createMockFlightClient({ authenticate: mockAuthenticate })
      const client = new FlightSqlClient(mockFlight)

      const result = await client.authenticate()

      expect(result).toBe("token123")
      expect(mockAuthenticate).toHaveBeenCalled()
    })
  })

  describe("getQueryInfo", () => {
    it("calls getFlightInfo with cmd descriptor", async () => {
      const mockFlightInfo = {
        endpoint: [{ ticket: { ticket: new Uint8Array() } }]
      }
      const mockGetFlightInfo = vi.fn().mockResolvedValue(mockFlightInfo)
      const mockFlight = createMockFlightClient({ getFlightInfo: mockGetFlightInfo })
      const client = new FlightSqlClient(mockFlight)

      const info = await client.getQueryInfo("SELECT * FROM users")

      expect(mockGetFlightInfo).toHaveBeenCalledWith(
        expect.objectContaining({
          cmd: expect.any(Uint8Array)
        })
      )
      expect(info).toEqual(mockFlightInfo)
    })
  })

  describe("queryStream", () => {
    it("fetches FlightInfo and streams data from endpoints", async () => {
      const mockFlightData = { dataHeader: new Uint8Array([1]), dataBody: new Uint8Array([2]) }
      const mockFlightInfo = {
        endpoint: [{ ticket: { ticket: new Uint8Array([1, 2, 3]) } }]
      }
      const mockGetFlightInfo = vi.fn().mockResolvedValue(mockFlightInfo)
      const mockDoGet = vi.fn().mockReturnValue(asyncIterable([mockFlightData]))
      const mockFlight = createMockFlightClient({
        getFlightInfo: mockGetFlightInfo,
        doGet: mockDoGet
      })
      const client = new FlightSqlClient(mockFlight)

      const dataMessages: unknown[] = []
      for await (const data of client.queryStream("SELECT * FROM users")) {
        dataMessages.push(data)
      }

      expect(mockGetFlightInfo).toHaveBeenCalled()
      expect(mockDoGet).toHaveBeenCalledWith({ ticket: new Uint8Array([1, 2, 3]) })
      expect(dataMessages).toHaveLength(1)
      expect(dataMessages[0]).toEqual(mockFlightData)
    })

    it("handles multiple endpoints", async () => {
      const mockFlightData1 = { dataHeader: new Uint8Array([1]) }
      const mockFlightData2 = { dataHeader: new Uint8Array([2]) }
      const mockFlightInfo = {
        endpoint: [
          { ticket: { ticket: new Uint8Array([1]) } },
          { ticket: { ticket: new Uint8Array([2]) } }
        ]
      }
      const mockGetFlightInfo = vi.fn().mockResolvedValue(mockFlightInfo)
      const mockDoGet = vi
        .fn()
        .mockReturnValueOnce(asyncIterable([mockFlightData1]))
        .mockReturnValueOnce(asyncIterable([mockFlightData2]))
      const mockFlight = createMockFlightClient({
        getFlightInfo: mockGetFlightInfo,
        doGet: mockDoGet
      })
      const client = new FlightSqlClient(mockFlight)

      const dataMessages: unknown[] = []
      for await (const data of client.queryStream("SELECT * FROM users")) {
        dataMessages.push(data)
      }

      expect(mockDoGet).toHaveBeenCalledTimes(2)
      expect(dataMessages).toHaveLength(2)
    })

    it("skips endpoints without tickets", async () => {
      const mockFlightData = { dataHeader: new Uint8Array([1]) }
      const mockFlightInfo = {
        endpoint: [
          { ticket: undefined },
          { ticket: { ticket: new Uint8Array([1]) } },
          { ticket: undefined }
        ]
      }
      const mockGetFlightInfo = vi.fn().mockResolvedValue(mockFlightInfo)
      const mockDoGet = vi.fn().mockReturnValue(asyncIterable([mockFlightData]))
      const mockFlight = createMockFlightClient({
        getFlightInfo: mockGetFlightInfo,
        doGet: mockDoGet
      })
      const client = new FlightSqlClient(mockFlight)

      for await (const _ of client.queryStream("SELECT * FROM users")) {
        // consume stream
      }

      expect(mockDoGet).toHaveBeenCalledTimes(1)
    })
  })

  describe("query", () => {
    it("returns a Table from decoded FlightData", async () => {
      const flightData = await createFlightDataArray()
      const mockFlightInfo = createMockFlightInfo()
      const mockGetFlightInfo = vi.fn().mockResolvedValue(mockFlightInfo)
      const mockDoGet = vi.fn().mockReturnValue(asyncIterable(flightData))
      const mockFlight = createMockFlightClient({
        getFlightInfo: mockGetFlightInfo,
        doGet: mockDoGet
      })
      const client = new FlightSqlClient(mockFlight)

      const table = await client.query("SELECT * FROM users")

      expect(mockGetFlightInfo).toHaveBeenCalled()
      expect(mockDoGet).toHaveBeenCalled()
      expect(table.numCols).toBe(2)
      expect(table.numRows).toBe(3)
      expect(table.schema.fields[0].name).toBe("id")
      expect(table.schema.fields[1].name).toBe("name")
    })
  })

  describe("queryBatches", () => {
    it("yields RecordBatches from decoded FlightData", async () => {
      const flightData = await createFlightDataArray()
      const mockFlightInfo = createMockFlightInfo()
      const mockGetFlightInfo = vi.fn().mockResolvedValue(mockFlightInfo)
      const mockDoGet = vi.fn().mockReturnValue(asyncIterable(flightData))
      const mockFlight = createMockFlightClient({
        getFlightInfo: mockGetFlightInfo,
        doGet: mockDoGet
      })
      const client = new FlightSqlClient(mockFlight)

      const batches = []
      for await (const batch of client.queryBatches("SELECT * FROM users")) {
        batches.push(batch)
      }

      expect(mockGetFlightInfo).toHaveBeenCalled()
      expect(mockDoGet).toHaveBeenCalled()
      expect(batches.length).toBeGreaterThan(0)
      expect(batches[0].schema.fields[0].name).toBe("id")
    })
  })

  describe("executeUpdate", () => {
    it("calls doPut and returns unknown count when no metadata", async () => {
      const mockDoPut = vi.fn().mockReturnValue(
        asyncIterable([
          {
            appMetadata: new Uint8Array() // Empty metadata = unknown count
          }
        ])
      )
      const mockFlight = createMockFlightClient({ doPut: mockDoPut })
      const client = new FlightSqlClient(mockFlight)

      const result = await client.executeUpdate("DELETE FROM users WHERE inactive = true")

      expect(mockDoPut).toHaveBeenCalled()
      expect(result.recordCount).toBe(-1n)
    })

    it("returns record count when metadata present", async () => {
      const mockUpdateResult = createMockUpdateResult(15n)
      const mockDoPut = vi.fn().mockReturnValue(asyncIterable([mockUpdateResult]))
      const mockFlight = createMockFlightClient({ doPut: mockDoPut })
      const client = new FlightSqlClient(mockFlight)

      const result = await client.executeUpdate("INSERT INTO users VALUES (1, 'test')")

      expect(mockDoPut).toHaveBeenCalled()
      expect(result.recordCount).toBe(15n)
    })
  })

  describe("prepare", () => {
    it("calls doAction with CreatePreparedStatement type", async () => {
      const mockDoAction = vi.fn().mockReturnValue(asyncIterable([]))
      const mockFlight = createMockFlightClient({ doAction: mockDoAction })
      const client = new FlightSqlClient(mockFlight)

      // The call will fail because no response, but we verify it was called
      await expect(client.prepare("SELECT * FROM users WHERE id = ?")).rejects.toThrow()

      expect(mockDoAction).toHaveBeenCalledWith({
        type: "CreatePreparedStatement",
        body: expect.any(Uint8Array)
      })
    })

    it("throws when no response received", async () => {
      const mockDoAction = vi.fn().mockReturnValue(asyncIterable([]))
      const mockFlight = createMockFlightClient({ doAction: mockDoAction })
      const client = new FlightSqlClient(mockFlight)

      await expect(client.prepare("SELECT * FROM users")).rejects.toThrow(FlightError)
      await expect(client.prepare("SELECT * FROM users")).rejects.toThrow(
        "failed to create prepared statement"
      )
    })

    it("returns PreparedStatement on success", async () => {
      const mockResponse = createMockPreparedStatementResponse()
      const mockDoAction = vi.fn().mockReturnValue(asyncIterable([mockResponse]))
      const mockFlight = createMockFlightClient({ doAction: mockDoAction })
      const client = new FlightSqlClient(mockFlight)

      const stmt = await client.prepare("SELECT * FROM users WHERE id = ?")

      expect(stmt.handle).toEqual(new Uint8Array([1, 2, 3, 4]))
      expect(stmt.datasetSchema).toBeInstanceOf(Uint8Array)
      expect(stmt.parameterSchema).toBeInstanceOf(Uint8Array)
    })
  })

  describe("closePreparedStatement", () => {
    it("calls doAction with ClosePreparedStatement type", async () => {
      const mockDoAction = vi.fn().mockReturnValue(asyncIterable([]))
      const mockFlight = createMockFlightClient({ doAction: mockDoAction })
      const client = new FlightSqlClient(mockFlight)

      const stmt = {
        handle: new Uint8Array([10, 20, 30]),
        datasetSchema: new Uint8Array(),
        parameterSchema: new Uint8Array()
      }

      await client.closePreparedStatement(stmt)

      expect(mockDoAction).toHaveBeenCalledWith({
        type: "ClosePreparedStatement",
        body: expect.any(Uint8Array)
      })
    })
  })

  describe("executePrepared", () => {
    it("returns a Table from decoded FlightData", async () => {
      const flightData = await createFlightDataArray()
      const mockFlightInfo = createMockFlightInfo()
      const mockGetFlightInfo = vi.fn().mockResolvedValue(mockFlightInfo)
      const mockDoGet = vi.fn().mockReturnValue(asyncIterable(flightData))
      const mockFlight = createMockFlightClient({
        getFlightInfo: mockGetFlightInfo,
        doGet: mockDoGet
      })
      const client = new FlightSqlClient(mockFlight)

      const stmt = {
        handle: new Uint8Array([1, 2, 3]),
        datasetSchema: new Uint8Array(),
        parameterSchema: new Uint8Array()
      }

      const table = await client.executePrepared(stmt)

      expect(mockGetFlightInfo).toHaveBeenCalled()
      expect(mockDoGet).toHaveBeenCalled()
      expect(table.numCols).toBe(2)
      expect(table.numRows).toBe(3)
    })
  })

  describe("executePreparedStream", () => {
    it("yields FlightData from endpoints", async () => {
      const flightData = await createFlightDataArray()
      const mockFlightInfo = createMockFlightInfo()
      const mockGetFlightInfo = vi.fn().mockResolvedValue(mockFlightInfo)
      const mockDoGet = vi.fn().mockReturnValue(asyncIterable(flightData))
      const mockFlight = createMockFlightClient({
        getFlightInfo: mockGetFlightInfo,
        doGet: mockDoGet
      })
      const client = new FlightSqlClient(mockFlight)

      const stmt = {
        handle: new Uint8Array([1, 2, 3]),
        datasetSchema: new Uint8Array(),
        parameterSchema: new Uint8Array()
      }

      const dataMessages = []
      for await (const data of client.executePreparedStream(stmt)) {
        dataMessages.push(data)
      }

      expect(mockGetFlightInfo).toHaveBeenCalled()
      expect(mockDoGet).toHaveBeenCalled()
      expect(dataMessages.length).toBeGreaterThan(0)
    })

    it("skips endpoints without tickets", async () => {
      const flightData = await createFlightDataArray()
      const mockFlightInfo = {
        endpoint: [{ ticket: undefined }, { ticket: { ticket: new Uint8Array([1, 2, 3]) } }]
      }
      const mockGetFlightInfo = vi.fn().mockResolvedValue(mockFlightInfo)
      const mockDoGet = vi.fn().mockReturnValue(asyncIterable(flightData))
      const mockFlight = createMockFlightClient({
        getFlightInfo: mockGetFlightInfo,
        doGet: mockDoGet
      })
      const client = new FlightSqlClient(mockFlight)

      const stmt = {
        handle: new Uint8Array([1, 2, 3]),
        datasetSchema: new Uint8Array(),
        parameterSchema: new Uint8Array()
      }

      const dataMessages = []
      for await (const data of client.executePreparedStream(stmt)) {
        dataMessages.push(data)
      }

      // Should only call doGet once (skip the endpoint without ticket)
      expect(mockDoGet).toHaveBeenCalledTimes(1)
    })
  })

  describe("executePreparedUpdate", () => {
    it("returns record count when metadata present", async () => {
      const mockUpdateResult = createMockUpdateResult(42n)
      const mockDoPut = vi.fn().mockReturnValue(asyncIterable([mockUpdateResult]))
      const mockFlight = createMockFlightClient({ doPut: mockDoPut })
      const client = new FlightSqlClient(mockFlight)

      const stmt = {
        handle: new Uint8Array([1, 2, 3]),
        datasetSchema: new Uint8Array(),
        parameterSchema: new Uint8Array()
      }

      const result = await client.executePreparedUpdate(stmt)

      expect(mockDoPut).toHaveBeenCalled()
      expect(result.recordCount).toBe(42n)
    })

    it("sends parameters when provided and consumes stream", async () => {
      const mockUpdateResult = createMockUpdateResult(5n)
      // Create a mock doPut that actually consumes the input stream
      const mockDoPut = vi.fn().mockImplementation(async function* (stream) {
        // Consume all FlightData from the stream to exercise withDescriptor path
        for await (const _ of stream) {
          // Consuming items
        }
        yield mockUpdateResult
      })
      const mockFlight = createMockFlightClient({ doPut: mockDoPut })
      const client = new FlightSqlClient(mockFlight)

      const stmt = {
        handle: new Uint8Array([1, 2, 3]),
        datasetSchema: new Uint8Array(),
        parameterSchema: new Uint8Array()
      }

      // Create parameter batches using async iterable with multiple batches
      // This exercises the withDescriptor generator path
      const paramTable1 = createTestTable()
      const paramTable2 = createTestTable()
      // eslint-disable-next-line @typescript-eslint/require-await, @typescript-eslint/explicit-function-return-type
      async function* asyncParams() {
        for (const batch of paramTable1.batches) {
          yield batch
        }
        for (const batch of paramTable2.batches) {
          yield batch
        }
      }

      const result = await client.executePreparedUpdate(stmt, asyncParams())

      expect(mockDoPut).toHaveBeenCalled()
      expect(result.recordCount).toBe(5n)
    })

    it("sends parameters from synchronous iterable", async () => {
      const mockUpdateResult = createMockUpdateResult(7n)
      const mockDoPut = vi.fn().mockReturnValue(asyncIterable([mockUpdateResult]))
      const mockFlight = createMockFlightClient({ doPut: mockDoPut })
      const client = new FlightSqlClient(mockFlight)

      const stmt = {
        handle: new Uint8Array([1, 2, 3]),
        datasetSchema: new Uint8Array(),
        parameterSchema: new Uint8Array()
      }

      // Create parameter batches from array (sync iterable)
      const paramTable = createTestTable()
      const params = paramTable.batches

      const result = await client.executePreparedUpdate(stmt, params)

      expect(mockDoPut).toHaveBeenCalled()
      expect(result.recordCount).toBe(7n)
    })

    it("handles empty parameters array", async () => {
      const mockUpdateResult = createMockUpdateResult(10n)
      const mockDoPut = vi.fn().mockReturnValue(asyncIterable([mockUpdateResult]))
      const mockFlight = createMockFlightClient({ doPut: mockDoPut })
      const client = new FlightSqlClient(mockFlight)

      const stmt = {
        handle: new Uint8Array([1, 2, 3]),
        datasetSchema: new Uint8Array(),
        parameterSchema: new Uint8Array()
      }

      // Pass empty array - should still work but use empty stream
      const result = await client.executePreparedUpdate(stmt, [])

      expect(mockDoPut).toHaveBeenCalled()
      expect(result.recordCount).toBe(10n)
    })
  })

  describe("beginTransaction", () => {
    it("calls doAction with BeginTransaction type", async () => {
      const mockDoAction = vi.fn().mockReturnValue(asyncIterable([]))
      const mockFlight = createMockFlightClient({ doAction: mockDoAction })
      const client = new FlightSqlClient(mockFlight)

      // The call will fail because no response, but we verify it was called
      await expect(client.beginTransaction()).rejects.toThrow()

      expect(mockDoAction).toHaveBeenCalledWith({
        type: "BeginTransaction",
        body: expect.any(Uint8Array)
      })
    })

    it("throws when no response received", async () => {
      const mockDoAction = vi.fn().mockReturnValue(asyncIterable([]))
      const mockFlight = createMockFlightClient({ doAction: mockDoAction })
      const client = new FlightSqlClient(mockFlight)

      await expect(client.beginTransaction()).rejects.toThrow(FlightError)
      await expect(client.beginTransaction()).rejects.toThrow("failed to begin transaction")
    })

    it("returns transaction on success", async () => {
      const mockResponse = createMockTransactionResponse()
      const mockDoAction = vi.fn().mockReturnValue(asyncIterable([mockResponse]))
      const mockFlight = createMockFlightClient({ doAction: mockDoAction })
      const client = new FlightSqlClient(mockFlight)

      const txn = await client.beginTransaction()

      expect(txn.id).toEqual(new Uint8Array([10, 20, 30, 40]))
    })
  })

  describe("commit", () => {
    it("calls doAction with EndTransaction type", async () => {
      const mockDoAction = vi.fn().mockReturnValue(asyncIterable([]))
      const mockFlight = createMockFlightClient({ doAction: mockDoAction })
      const client = new FlightSqlClient(mockFlight)

      const txn = { id: new Uint8Array([100, 101, 102]) }

      await client.commit(txn)

      expect(mockDoAction).toHaveBeenCalledWith({
        type: "EndTransaction",
        body: expect.any(Uint8Array)
      })
    })
  })

  describe("rollback", () => {
    it("calls doAction with EndTransaction type", async () => {
      const mockDoAction = vi.fn().mockReturnValue(asyncIterable([]))
      const mockFlight = createMockFlightClient({ doAction: mockDoAction })
      const client = new FlightSqlClient(mockFlight)

      const txn = { id: new Uint8Array([100, 101, 102]) }

      await client.rollback(txn)

      expect(mockDoAction).toHaveBeenCalledWith({
        type: "EndTransaction",
        body: expect.any(Uint8Array)
      })
    })
  })

  describe("getCatalogs", () => {
    it("calls getFlightInfo with cmd descriptor", async () => {
      const mockFlightInfo = {
        endpoint: [{ ticket: { ticket: new Uint8Array([1, 2, 3]) } }]
      }
      const mockGetFlightInfo = vi.fn().mockResolvedValue(mockFlightInfo)
      const mockDoGet = vi.fn().mockReturnValue(asyncIterable([]))
      const mockFlight = createMockFlightClient({
        getFlightInfo: mockGetFlightInfo,
        doGet: mockDoGet
      })
      const client = new FlightSqlClient(mockFlight)

      // Will throw due to no record batches, but we verify delegation happened
      await expect(client.getCatalogs()).rejects.toThrow()

      expect(mockGetFlightInfo).toHaveBeenCalledWith(
        expect.objectContaining({
          cmd: expect.any(Uint8Array)
        })
      )
    })

    it("returns a Table on success", async () => {
      const flightData = await createFlightDataArray()
      const mockFlightInfo = createMockFlightInfo()
      const mockGetFlightInfo = vi.fn().mockResolvedValue(mockFlightInfo)
      const mockDoGet = vi.fn().mockReturnValue(asyncIterable(flightData))
      const mockFlight = createMockFlightClient({
        getFlightInfo: mockGetFlightInfo,
        doGet: mockDoGet
      })
      const client = new FlightSqlClient(mockFlight)

      const table = await client.getCatalogs()

      expect(mockGetFlightInfo).toHaveBeenCalled()
      expect(mockDoGet).toHaveBeenCalled()
      expect(table.numCols).toBeGreaterThan(0)
    })

    it("skips endpoints without tickets", async () => {
      const flightData = await createFlightDataArray()
      const mockFlightInfo = {
        endpoint: [{ ticket: undefined }, { ticket: { ticket: new Uint8Array([1, 2, 3]) } }]
      }
      const mockGetFlightInfo = vi.fn().mockResolvedValue(mockFlightInfo)
      const mockDoGet = vi.fn().mockReturnValue(asyncIterable(flightData))
      const mockFlight = createMockFlightClient({
        getFlightInfo: mockGetFlightInfo,
        doGet: mockDoGet
      })
      const client = new FlightSqlClient(mockFlight)

      const table = await client.getCatalogs()

      // Should only call doGet once (skipping the endpoint without ticket)
      expect(mockDoGet).toHaveBeenCalledTimes(1)
      expect(table.numCols).toBeGreaterThan(0)
    })
  })

  describe("getDbSchemas", () => {
    it("calls getFlightInfo with cmd descriptor", async () => {
      const mockFlightInfo = {
        endpoint: [{ ticket: { ticket: new Uint8Array([1, 2, 3]) } }]
      }
      const mockGetFlightInfo = vi.fn().mockResolvedValue(mockFlightInfo)
      const mockDoGet = vi.fn().mockReturnValue(asyncIterable([]))
      const mockFlight = createMockFlightClient({
        getFlightInfo: mockGetFlightInfo,
        doGet: mockDoGet
      })
      const client = new FlightSqlClient(mockFlight)

      // Will throw due to no record batches, but we verify delegation happened
      await expect(client.getDbSchemas({ catalog: "my_catalog" })).rejects.toThrow()

      expect(mockGetFlightInfo).toHaveBeenCalledWith(
        expect.objectContaining({
          cmd: expect.any(Uint8Array)
        })
      )
    })
  })

  describe("getTables", () => {
    it("calls getFlightInfo with cmd descriptor", async () => {
      const mockFlightInfo = {
        endpoint: [{ ticket: { ticket: new Uint8Array([1, 2, 3]) } }]
      }
      const mockGetFlightInfo = vi.fn().mockResolvedValue(mockFlightInfo)
      const mockDoGet = vi.fn().mockReturnValue(asyncIterable([]))
      const mockFlight = createMockFlightClient({
        getFlightInfo: mockGetFlightInfo,
        doGet: mockDoGet
      })
      const client = new FlightSqlClient(mockFlight)

      // Will throw due to no record batches, but we verify delegation happened
      await expect(client.getTables({ tableTypes: ["TABLE", "VIEW"] })).rejects.toThrow()

      expect(mockGetFlightInfo).toHaveBeenCalledWith(
        expect.objectContaining({
          cmd: expect.any(Uint8Array)
        })
      )
    })
  })

  describe("getTableTypes", () => {
    it("calls getFlightInfo with cmd descriptor", async () => {
      const mockFlightInfo = {
        endpoint: [{ ticket: { ticket: new Uint8Array([1, 2, 3]) } }]
      }
      const mockGetFlightInfo = vi.fn().mockResolvedValue(mockFlightInfo)
      const mockDoGet = vi.fn().mockReturnValue(asyncIterable([]))
      const mockFlight = createMockFlightClient({
        getFlightInfo: mockGetFlightInfo,
        doGet: mockDoGet
      })
      const client = new FlightSqlClient(mockFlight)

      // Will throw due to no record batches, but we verify delegation happened
      await expect(client.getTableTypes()).rejects.toThrow()

      expect(mockGetFlightInfo).toHaveBeenCalledWith(
        expect.objectContaining({
          cmd: expect.any(Uint8Array)
        })
      )
    })
  })

  describe("getPrimaryKeys", () => {
    it("calls getFlightInfo with cmd descriptor", async () => {
      const mockFlightInfo = {
        endpoint: [{ ticket: { ticket: new Uint8Array([1, 2, 3]) } }]
      }
      const mockGetFlightInfo = vi.fn().mockResolvedValue(mockFlightInfo)
      const mockDoGet = vi.fn().mockReturnValue(asyncIterable([]))
      const mockFlight = createMockFlightClient({
        getFlightInfo: mockGetFlightInfo,
        doGet: mockDoGet
      })
      const client = new FlightSqlClient(mockFlight)

      // Will throw due to no record batches, but we verify delegation happened
      await expect(
        client.getPrimaryKeys("users", { catalog: "my_catalog", dbSchema: "public" })
      ).rejects.toThrow()

      expect(mockGetFlightInfo).toHaveBeenCalledWith(
        expect.objectContaining({
          cmd: expect.any(Uint8Array)
        })
      )
    })
  })
})
