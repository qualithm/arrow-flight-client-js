import { create, fromBinary, toBinary } from "@bufbuild/protobuf"
import type { RecordBatch, Table, TypeMap } from "apache-arrow"

import type { FlightData, FlightInfo } from "../gen/arrow/flight/Flight_pb.js"
import {
  ActionBeginTransactionRequestSchema,
  type ActionBeginTransactionResult,
  ActionBeginTransactionResultSchema,
  ActionClosePreparedStatementRequestSchema,
  ActionCreatePreparedStatementRequestSchema,
  type ActionCreatePreparedStatementResult,
  ActionCreatePreparedStatementResultSchema,
  ActionEndTransactionRequest_EndTransaction,
  ActionEndTransactionRequestSchema,
  CommandGetCatalogsSchema,
  CommandGetDbSchemasSchema,
  CommandGetPrimaryKeysSchema,
  CommandGetTablesSchema,
  CommandGetTableTypesSchema,
  CommandPreparedStatementQuerySchema,
  CommandPreparedStatementUpdateSchema,
  CommandStatementQuerySchema,
  CommandStatementUpdateSchema,
  type DoPutUpdateResult,
  DoPutUpdateResultSchema
} from "../gen/arrow/flight/FlightSql_pb.js"
import { FlightError } from "./errors.js"
import { FlightClient } from "./flight-client.js"
import {
  decodeFlightDataStream,
  decodeFlightDataToTable,
  encodeRecordBatchesToFlightData
} from "./ipc.js"
import type { FlightClientOptions, FlightDescriptorInput } from "./types.js"

/**
 * Flight SQL action type identifiers.
 */
const FLIGHT_SQL_ACTIONS = {
  CREATE_PREPARED_STATEMENT: "CreatePreparedStatement",
  CLOSE_PREPARED_STATEMENT: "ClosePreparedStatement",
  BEGIN_TRANSACTION: "BeginTransaction",
  END_TRANSACTION: "EndTransaction"
} as const

/**
 * Options for executing a SQL query.
 */
export type ExecuteQueryOptions = {
  /**
   * Execute the query within this transaction.
   * If not provided, the query is auto-committed.
   */
  transactionId?: Uint8Array
}

/**
 * Options for executing a SQL update (INSERT, UPDATE, DELETE).
 */
export type ExecuteUpdateOptions = {
  /**
   * Execute the update within this transaction.
   * If not provided, the update is auto-committed.
   */
  transactionId?: Uint8Array
}

/**
 * Result of executing a SQL update.
 */
export type UpdateResult = {
  /**
   * Number of records affected by the update.
   * -1 indicates an unknown count.
   */
  recordCount: bigint
}

/**
 * Prepared statement handle returned from preparing a SQL statement.
 */
export type PreparedStatement = {
  /**
   * Opaque handle for the prepared statement on the server.
   */
  handle: Uint8Array

  /**
   * Schema of the result set (IPC-encoded).
   * Empty if the statement doesn't return results.
   */
  datasetSchema: Uint8Array

  /**
   * Schema of the parameters (IPC-encoded).
   * Empty if the statement has no parameters.
   */
  parameterSchema: Uint8Array
}

/**
 * Transaction handle returned from beginning a transaction.
 */
export type Transaction = {
  /**
   * Opaque handle for the transaction on the server.
   */
  id: Uint8Array
}

/**
 * Arrow Flight SQL client for executing SQL queries and managing transactions.
 *
 * FlightSqlClient wraps FlightClient using composition, providing Flight SQL
 * specific operations while delegating core Flight RPC to the underlying client.
 *
 * @example
 * ```ts
 * const client = new FlightSqlClient({ url: "https://flight.example.com:8815" })
 *
 * // Execute a query and get results as a Table
 * const table = await client.query("SELECT * FROM users")
 * console.log(`Got ${table.numRows} rows`)
 *
 * // Execute an update
 * const result = await client.executeUpdate("INSERT INTO users (name) VALUES ('Alice')")
 * console.log(`Inserted ${result.recordCount} rows`)
 *
 * // Use prepared statements
 * const stmt = await client.prepare("SELECT * FROM users WHERE id = ?")
 * // ... bind parameters and execute ...
 * await client.closePreparedStatement(stmt)
 *
 * client.close()
 * ```
 */
export class FlightSqlClient {
  readonly #flight: FlightClient

  /**
   * Creates a new FlightSqlClient.
   *
   * @param optionsOrClient - Either FlightClientOptions to create a new FlightClient,
   *                          or an existing FlightClient instance to wrap.
   */
  constructor(optionsOrClient: FlightClientOptions | FlightClient) {
    // Duck-type check: FlightClient has getFlightInfo method, options don't
    if (
      typeof (optionsOrClient as FlightClient).getFlightInfo === "function" &&
      typeof (optionsOrClient as FlightClient).doGet === "function"
    ) {
      this.#flight = optionsOrClient as FlightClient
    } else {
      this.#flight = new FlightClient(optionsOrClient as FlightClientOptions)
    }
  }

  /**
   * The underlying Flight client.
   * Use this for advanced Flight operations not covered by Flight SQL.
   */
  get flight(): FlightClient {
    return this.#flight
  }

  /**
   * The base URL of the Flight server.
   */
  get url(): string {
    return this.#flight.url
  }

  /**
   * Whether the client has been closed.
   */
  get closed(): boolean {
    return this.#flight.closed
  }

  /**
   * Close the client and release resources.
   */
  close(): void {
    this.#flight.close()
  }

  /**
   * Authenticate with the server using configured credentials.
   */
  async authenticate(): Promise<string | undefined> {
    return this.#flight.authenticate()
  }

  // ── SQL Query Execution ─────────────────────────────────────────────

  /**
   * Execute a SQL query and return results as an Arrow Table.
   *
   * This is a convenience method that combines getFlightInfo and doGet
   * to fetch all query results into memory.
   *
   * @param query - SQL query string
   * @param options - Query execution options
   * @returns Arrow Table containing query results
   *
   * @example
   * ```ts
   * const table = await client.query("SELECT * FROM users WHERE active = true")
   * console.log(`Found ${table.numRows} active users`)
   * ```
   */
  async query<T extends TypeMap = TypeMap>(
    query: string,
    options?: ExecuteQueryOptions
  ): Promise<Table<T>> {
    const stream = this.queryStream(query, options)
    return decodeFlightDataToTable(stream)
  }

  /**
   * Execute a SQL query and return results as a stream of RecordBatches.
   *
   * Use this for large result sets to avoid loading all data into memory.
   *
   * @param query - SQL query string
   * @param options - Query execution options
   * @yields RecordBatch objects
   *
   * @example
   * ```ts
   * for await (const batch of client.queryBatches("SELECT * FROM large_table")) {
   *   processBatch(batch)
   * }
   * ```
   */
  async *queryBatches<T extends TypeMap = TypeMap>(
    query: string,
    options?: ExecuteQueryOptions
  ): AsyncGenerator<RecordBatch<T>> {
    const stream = this.queryStream(query, options)
    for await (const batch of decodeFlightDataStream<T>(stream)) {
      yield batch
    }
  }

  /**
   * Execute a SQL query and return the raw FlightData stream.
   *
   * This is the lowest-level query method, useful when you need
   * access to raw Flight data or custom decoding.
   *
   * @param query - SQL query string
   * @param options - Query execution options
   * @yields FlightData messages
   */
  async *queryStream(query: string, options?: ExecuteQueryOptions): AsyncGenerator<FlightData> {
    // Create CommandStatementQuery
    const command = create(CommandStatementQuerySchema, {
      query,
      transactionId: options?.transactionId
    })

    // Serialize command to bytes
    const cmdBytes = toBinary(CommandStatementQuerySchema, command)

    // Get flight info for the query
    const descriptor: FlightDescriptorInput = { type: "cmd", cmd: cmdBytes }
    const flightInfo = await this.#flight.getFlightInfo(descriptor)

    // Fetch data from each endpoint
    for (const endpoint of flightInfo.endpoint) {
      if (!endpoint.ticket) {
        continue
      }

      // Yield FlightData from this endpoint
      for await (const data of this.#flight.doGet(endpoint.ticket)) {
        yield data
      }
    }
  }

  /**
   * Get FlightInfo for a SQL query without executing it.
   *
   * Use this to inspect the query plan, schema, or endpoints
   * before fetching data.
   *
   * @param query - SQL query string
   * @param options - Query execution options
   * @returns FlightInfo describing the query results
   */
  async getQueryInfo(query: string, options?: ExecuteQueryOptions): Promise<FlightInfo> {
    const command = create(CommandStatementQuerySchema, {
      query,
      transactionId: options?.transactionId
    })
    const cmdBytes = toBinary(CommandStatementQuerySchema, command)
    return this.#flight.getFlightInfo({ type: "cmd", cmd: cmdBytes })
  }

  // ── SQL Update Execution ────────────────────────────────────────────

  /**
   * Execute a SQL update statement (INSERT, UPDATE, DELETE, DDL).
   *
   * @param query - SQL update statement
   * @param options - Update execution options
   * @returns Update result with affected row count
   *
   * @example
   * ```ts
   * const result = await client.executeUpdate("DELETE FROM users WHERE inactive = true")
   * console.log(`Deleted ${result.recordCount} inactive users`)
   * ```
   */
  async executeUpdate(query: string, options?: ExecuteUpdateOptions): Promise<UpdateResult> {
    // Create CommandStatementUpdate
    const command = create(CommandStatementUpdateSchema, {
      query,
      transactionId: options?.transactionId
    })

    const cmdBytes = toBinary(CommandStatementUpdateSchema, command)

    // For updates, we use DoPut with the command as descriptor

    // Create empty FlightData stream with descriptor
    // eslint-disable-next-line @typescript-eslint/require-await
    async function* emptyStream(): AsyncGenerator<FlightData> {
      yield {
        flightDescriptor: {
          type: 2, // CMD
          cmd: cmdBytes,
          path: []
        },
        dataHeader: new Uint8Array(),
        dataBody: new Uint8Array(),
        appMetadata: new Uint8Array()
      } as unknown as FlightData
    }

    // Execute DoPut and collect result
    let updateResult: DoPutUpdateResult | undefined
    for await (const result of this.#flight.doPut(emptyStream())) {
      // Parse the result as DoPutUpdateResult
      if (result.appMetadata.length > 0) {
        updateResult = fromBinary(DoPutUpdateResultSchema, result.appMetadata)
      }
    }

    return {
      recordCount: updateResult?.recordCount ?? -1n
    }
  }

  // ── Prepared Statements ─────────────────────────────────────────────

  /**
   * Prepare a SQL statement for repeated execution.
   *
   * @param query - SQL query with optional parameter placeholders
   * @param transactionId - Optional transaction to associate with the statement
   * @returns Prepared statement handle
   *
   * @example
   * ```ts
   * const stmt = await client.prepare("SELECT * FROM users WHERE id = ?")
   * // Use executePrepared() to execute with parameters
   * await client.closePreparedStatement(stmt)
   * ```
   */
  async prepare(query: string, transactionId?: Uint8Array): Promise<PreparedStatement> {
    const request = create(ActionCreatePreparedStatementRequestSchema, {
      query,
      transactionId
    })

    const requestBytes = toBinary(ActionCreatePreparedStatementRequestSchema, request)

    let result: ActionCreatePreparedStatementResult | undefined
    for await (const response of this.#flight.doAction({
      type: FLIGHT_SQL_ACTIONS.CREATE_PREPARED_STATEMENT,
      body: requestBytes
    })) {
      result = fromBinary(ActionCreatePreparedStatementResultSchema, response.body)
      break
    }

    if (!result) {
      throw new FlightError("failed to create prepared statement: no response from server")
    }

    return {
      handle: result.preparedStatementHandle,
      datasetSchema: result.datasetSchema,
      parameterSchema: result.parameterSchema
    }
  }

  /**
   * Execute a prepared query statement and return results as a Table.
   *
   * @param statement - Prepared statement from prepare()
   * @returns Arrow Table containing query results
   */
  async executePrepared<T extends TypeMap = TypeMap>(
    statement: PreparedStatement
  ): Promise<Table<T>> {
    const stream = this.executePreparedStream(statement)
    return decodeFlightDataToTable(stream)
  }

  /**
   * Execute a prepared query statement and return results as a stream.
   *
   * @param statement - Prepared statement from prepare()
   * @yields FlightData messages
   */
  async *executePreparedStream(statement: PreparedStatement): AsyncGenerator<FlightData> {
    const command = create(CommandPreparedStatementQuerySchema, {
      preparedStatementHandle: statement.handle
    })

    const cmdBytes = toBinary(CommandPreparedStatementQuerySchema, command)
    const flightInfo = await this.#flight.getFlightInfo({ type: "cmd", cmd: cmdBytes })

    for (const endpoint of flightInfo.endpoint) {
      if (!endpoint.ticket) {
        continue
      }
      for await (const data of this.#flight.doGet(endpoint.ticket)) {
        yield data
      }
    }
  }

  /**
   * Execute a prepared update statement and return affected row count.
   *
   * @param statement - Prepared statement from prepare()
   * @param parameters - Optional parameter values as RecordBatches
   * @returns Update result with affected row count
   */
  async executePreparedUpdate(
    statement: PreparedStatement,
    parameters?: AsyncIterable<RecordBatch> | Iterable<RecordBatch>
  ): Promise<UpdateResult> {
    const command = create(CommandPreparedStatementUpdateSchema, {
      preparedStatementHandle: statement.handle
    })

    const cmdBytes = toBinary(CommandPreparedStatementUpdateSchema, command)

    // If parameters provided, send them via DoPut
    // Otherwise send empty stream
    let dataStream: AsyncGenerator<FlightData>

    if (parameters) {
      // We need a schema to encode - for now we'll collect batches and use first batch's schema
      const batchArray: RecordBatch[] = []
      for await (const batch of parameters) {
        batchArray.push(batch)
      }

      if (batchArray.length > 0) {
        const { schema } = batchArray[0]

        // Wrap encoded data with descriptor
        async function* withDescriptor(
          encoded: AsyncIterable<FlightData>
        ): AsyncGenerator<FlightData> {
          let first = true
          for await (const data of encoded) {
            if (first) {
              yield {
                ...data,
                flightDescriptor: { type: 2, cmd: cmdBytes, path: [] }
              } as unknown as FlightData
              first = false
            } else {
              yield data
            }
          }
        }

        dataStream = withDescriptor(encodeRecordBatchesToFlightData(batchArray, schema))
      } else {
        dataStream = this.#createEmptyStream(cmdBytes)
      }
    } else {
      dataStream = this.#createEmptyStream(cmdBytes)
    }

    let updateResult: DoPutUpdateResult | undefined
    for await (const result of this.#flight.doPut(dataStream)) {
      if (result.appMetadata.length > 0) {
        updateResult = fromBinary(DoPutUpdateResultSchema, result.appMetadata)
      }
    }

    return {
      recordCount: updateResult?.recordCount ?? -1n
    }
  }

  /**
   * Close a prepared statement and release server resources.
   *
   * @param statement - Prepared statement to close
   */
  async closePreparedStatement(statement: PreparedStatement): Promise<void> {
    const request = create(ActionClosePreparedStatementRequestSchema, {
      preparedStatementHandle: statement.handle
    })

    const requestBytes = toBinary(ActionClosePreparedStatementRequestSchema, request)

    // Fire and forget - no response expected
    for await (const _ of this.#flight.doAction({
      type: FLIGHT_SQL_ACTIONS.CLOSE_PREPARED_STATEMENT,
      body: requestBytes
    })) {
      // Consume stream
    }
  }

  // ── Transactions ────────────────────────────────────────────────────

  /**
   * Begin a new transaction.
   *
   * @returns Transaction handle
   *
   * @example
   * ```ts
   * const txn = await client.beginTransaction()
   * try {
   *   await client.executeUpdate("INSERT INTO users ...", { transactionId: txn.id })
   *   await client.executeUpdate("INSERT INTO logs ...", { transactionId: txn.id })
   *   await client.commit(txn)
   * } catch (e) {
   *   await client.rollback(txn)
   *   throw e
   * }
   * ```
   */
  async beginTransaction(): Promise<Transaction> {
    const request = create(ActionBeginTransactionRequestSchema, {})
    const requestBytes = toBinary(ActionBeginTransactionRequestSchema, request)

    let result: ActionBeginTransactionResult | undefined
    for await (const response of this.#flight.doAction({
      type: FLIGHT_SQL_ACTIONS.BEGIN_TRANSACTION,
      body: requestBytes
    })) {
      result = fromBinary(ActionBeginTransactionResultSchema, response.body)
      break
    }

    if (!result) {
      throw new FlightError("failed to begin transaction: no response from server")
    }

    return {
      id: result.transactionId
    }
  }

  /**
   * Commit a transaction.
   *
   * @param transaction - Transaction to commit
   */
  async commit(transaction: Transaction): Promise<void> {
    await this.#endTransaction(transaction, ActionEndTransactionRequest_EndTransaction.COMMIT)
  }

  /**
   * Roll back a transaction.
   *
   * @param transaction - Transaction to roll back
   */
  async rollback(transaction: Transaction): Promise<void> {
    await this.#endTransaction(transaction, ActionEndTransactionRequest_EndTransaction.ROLLBACK)
  }

  // ── Database Metadata ───────────────────────────────────────────────

  /**
   * Get the list of catalogs in the database.
   *
   * @returns Arrow Table with catalog_name column
   *
   * @example
   * ```ts
   * const catalogs = await client.getCatalogs()
   * for (const row of catalogs) {
   *   console.log(row.catalog_name)
   * }
   * ```
   */
  async getCatalogs<T extends TypeMap = TypeMap>(): Promise<Table<T>> {
    const command = create(CommandGetCatalogsSchema, {})
    const cmdBytes = toBinary(CommandGetCatalogsSchema, command)
    return this.#executeMetadataQuery<T>(cmdBytes)
  }

  /**
   * Get the list of database schemas.
   *
   * @param options - Filter options
   * @returns Arrow Table with catalog_name and db_schema_name columns
   *
   * @example
   * ```ts
   * const schemas = await client.getDbSchemas({ catalog: "my_catalog" })
   * ```
   */
  async getDbSchemas<T extends TypeMap = TypeMap>(options?: {
    /** Filter by catalog name */
    catalog?: string
    /** Filter pattern for schema names (supports % and _ wildcards) */
    dbSchemaFilterPattern?: string
  }): Promise<Table<T>> {
    const command = create(CommandGetDbSchemasSchema, {
      catalog: options?.catalog,
      dbSchemaFilterPattern: options?.dbSchemaFilterPattern
    })
    const cmdBytes = toBinary(CommandGetDbSchemasSchema, command)
    return this.#executeMetadataQuery<T>(cmdBytes)
  }

  /**
   * Get the list of tables.
   *
   * @param options - Filter options
   * @returns Arrow Table with catalog_name, db_schema_name, table_name, table_type columns
   *
   * @example
   * ```ts
   * const tables = await client.getTables({ tableTypes: ["TABLE", "VIEW"] })
   * ```
   */
  async getTables<T extends TypeMap = TypeMap>(options?: {
    /** Filter by catalog name */
    catalog?: string
    /** Filter pattern for schema names (supports % and _ wildcards) */
    dbSchemaFilterPattern?: string
    /** Filter pattern for table names (supports % and _ wildcards) */
    tableNameFilterPattern?: string
    /** Filter by table types (e.g., "TABLE", "VIEW") */
    tableTypes?: string[]
    /** Include table schema in results */
    includeSchema?: boolean
  }): Promise<Table<T>> {
    const command = create(CommandGetTablesSchema, {
      catalog: options?.catalog,
      dbSchemaFilterPattern: options?.dbSchemaFilterPattern,
      tableNameFilterPattern: options?.tableNameFilterPattern,
      tableTypes: options?.tableTypes ?? [],
      includeSchema: options?.includeSchema ?? false
    })
    const cmdBytes = toBinary(CommandGetTablesSchema, command)
    return this.#executeMetadataQuery<T>(cmdBytes)
  }

  /**
   * Get the list of table types supported by the server.
   *
   * @returns Arrow Table with table_type column
   *
   * @example
   * ```ts
   * const tableTypes = await client.getTableTypes()
   * // Common types: TABLE, VIEW, SYSTEM TABLE
   * ```
   */
  async getTableTypes<T extends TypeMap = TypeMap>(): Promise<Table<T>> {
    const command = create(CommandGetTableTypesSchema, {})
    const cmdBytes = toBinary(CommandGetTableTypesSchema, command)
    return this.#executeMetadataQuery<T>(cmdBytes)
  }

  /**
   * Get the primary keys for a table.
   *
   * @param table - Table name (required)
   * @param options - Additional filter options
   * @returns Arrow Table with catalog_name, db_schema_name, table_name, column_name, key_name, key_sequence columns
   *
   * @example
   * ```ts
   * const keys = await client.getPrimaryKeys("users", { catalog: "my_db" })
   * ```
   */
  async getPrimaryKeys<T extends TypeMap = TypeMap>(
    table: string,
    options?: {
      /** Catalog containing the table */
      catalog?: string
      /** Schema containing the table */
      dbSchema?: string
    }
  ): Promise<Table<T>> {
    const command = create(CommandGetPrimaryKeysSchema, {
      table,
      catalog: options?.catalog,
      dbSchema: options?.dbSchema
    })
    const cmdBytes = toBinary(CommandGetPrimaryKeysSchema, command)
    return this.#executeMetadataQuery<T>(cmdBytes)
  }

  // ── Private Helpers ─────────────────────────────────────────────────

  /**
   * Execute a metadata query and return the results as a Table.
   */
  async #executeMetadataQuery<T extends TypeMap = TypeMap>(
    cmdBytes: Uint8Array
  ): Promise<Table<T>> {
    const descriptor: FlightDescriptorInput = { type: "cmd", cmd: cmdBytes }
    const flightInfo = await this.#flight.getFlightInfo(descriptor)

    // Collect all data from all endpoints
    const allData: FlightData[] = []
    for (const endpoint of flightInfo.endpoint) {
      if (!endpoint.ticket) {
        continue
      }
      for await (const data of this.#flight.doGet(endpoint.ticket)) {
        allData.push(data)
      }
    }

    // Convert to async iterable for decoding
    // eslint-disable-next-line @typescript-eslint/require-await
    async function* toAsyncIterable(items: FlightData[]): AsyncIterable<FlightData> {
      for (const item of items) {
        yield item
      }
    }

    return decodeFlightDataToTable<T>(toAsyncIterable(allData))
  }

  async #endTransaction(
    transaction: Transaction,
    action: ActionEndTransactionRequest_EndTransaction
  ): Promise<void> {
    const request = create(ActionEndTransactionRequestSchema, {
      transactionId: transaction.id,
      action
    })

    const requestBytes = toBinary(ActionEndTransactionRequestSchema, request)

    for await (const _ of this.#flight.doAction({
      type: FLIGHT_SQL_ACTIONS.END_TRANSACTION,
      body: requestBytes
    })) {
      // Consume stream
    }
  }

  // eslint-disable-next-line @typescript-eslint/require-await
  async *#createEmptyStream(cmdBytes: Uint8Array): AsyncGenerator<FlightData> {
    yield {
      flightDescriptor: { type: 2, cmd: cmdBytes, path: [] },
      dataHeader: new Uint8Array(),
      dataBody: new Uint8Array(),
      appMetadata: new Uint8Array()
    } as unknown as FlightData
  }
}
