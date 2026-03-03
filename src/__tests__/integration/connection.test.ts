/**
 * Integration tests for Flight client connection and handshake.
 *
 * Requires a running Arrow Flight server.
 *
 * @example
 * ```bash
 * # Start your Arrow Flight server, then run:
 * bun test src/__tests__/integration
 * ```
 */
import { afterEach, describe, expect, it } from "vitest"

import { createFlightClient, FlightClient } from "../../client"
import { config } from "./config"

describe("FlightClient Integration", () => {
  let client: FlightClient | null = null

  afterEach(() => {
    if (client !== null) {
      client.close()
      client = null
    }
  })

  describe("connection", () => {
    it("creates a client without throwing", () => {
      client = new FlightClient({ url: config.url })

      expect(client.url).toBe(config.url)
      expect(client.closed).toBe(false)
    })

    it("creates using createFlightClient helper", () => {
      client = createFlightClient({ url: config.url })

      expect(client.url).toBe(config.url)
      expect(client.closed).toBe(false)
    })

    it("closes the client", () => {
      client = new FlightClient({ url: config.url })

      expect(client.closed).toBe(false)
      client.close()
      expect(client.closed).toBe(true)
    })
  })

  describe("handshake", () => {
    it("performs basic auth handshake with valid credentials", async () => {
      client = new FlightClient({
        url: config.url,
        auth: {
          type: "basic",
          credentials: config.credentials.admin
        }
      })

      const token = await client.handshake()

      expect(token).toBeDefined()
      expect(token.length).toBeGreaterThan(0)
      expect(client.authenticated).toBe(true)
    })

    it("performs handshake with reader credentials", async () => {
      client = new FlightClient({
        url: config.url,
        auth: {
          type: "basic",
          credentials: config.credentials.reader
        }
      })

      const token = await client.handshake()

      expect(token).toBeDefined()
      expect(token.length).toBeGreaterThan(0)
    })

    it("rejects invalid credentials", async () => {
      client = new FlightClient({
        url: config.url,
        auth: {
          type: "basic",
          credentials: config.credentials.invalid
        }
      })

      await expect(client.handshake()).rejects.toThrow()
    })

    it("authenticates with basic auth via authenticate()", async () => {
      client = new FlightClient({
        url: config.url,
        auth: {
          type: "basic",
          credentials: config.credentials.admin
        }
      })

      const token = await client.authenticate()

      expect(token).toBeDefined()
      expect(client.authenticated).toBe(true)
    })

    it("bearer auth sets authenticated without RPC", async () => {
      client = new FlightClient({
        url: config.url,
        auth: {
          type: "bearer",
          token: "test-bearer-token"
        }
      })

      const token = await client.authenticate()

      expect(token).toBe("test-bearer-token")
      expect(client.authenticated).toBe(true)
    })
  })
})
