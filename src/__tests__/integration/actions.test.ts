/**
 * Integration tests for Flight actions: doAction, listActions.
 *
 * Requires a running Arrow Flight server.
 */
import { afterAll, beforeAll, describe, expect, it } from "vitest"

import { createFlightClient, type FlightClient } from "../../client"
import type { ActionType, Result } from "../../gen/arrow/flight/Flight_pb"
import { config } from "./config"

describe("Actions Integration", () => {
  let client: FlightClient

  beforeAll(async () => {
    client = createFlightClient({
      url: config.url,
      auth: {
        type: "basic",
        credentials: config.credentials.admin
      }
    })
    await client.authenticate()
  })

  afterAll(() => {
    client.close()
  })

  describe("listActions", () => {
    it("lists available actions", async () => {
      const actions: ActionType[] = []

      for await (const action of client.listActions()) {
        actions.push(action)
      }

      // Server should have at least some actions
      expect(actions.length).toBeGreaterThan(0)
    })

    it("returns action descriptions", async () => {
      for await (const action of client.listActions()) {
        expect(action.type).toBeDefined()
        expect(action.type.length).toBeGreaterThan(0)
        // Description may be empty but should be defined
        expect(action.description).toBeDefined()
      }
    })
  })

  describe("doAction", () => {
    it("executes healthcheck action", async () => {
      const results: Result[] = []

      for await (const result of client.doAction({
        type: "healthcheck",
        body: new Uint8Array()
      })) {
        results.push(result)
      }

      expect(results.length).toBe(1)

      // Parse the JSON response
      const body = JSON.parse(new TextDecoder().decode(results[0].body)) as { status: string }
      expect(body.status).toBe("ok")
    })

    it("executes echo action", async () => {
      const testPayload = new TextEncoder().encode("Hello, Flight!")
      const results: Result[] = []

      for await (const result of client.doAction({
        type: "echo",
        body: testPayload
      })) {
        results.push(result)
      }

      expect(results.length).toBe(1)
      // Echo should return the same bytes
      expect(new Uint8Array(results[0].body)).toEqual(testPayload)
    })

    it("error action returns error", async () => {
      await expect(
        (async () => {
          for await (const _ of client.doAction({
            type: "error",
            body: new Uint8Array()
          })) {
            // Should not reach here
          }
        })()
      ).rejects.toThrow()
    })

    it("returns error for unknown action", async () => {
      await expect(
        (async () => {
          for await (const _ of client.doAction({
            type: "unknown-action-that-does-not-exist",
            body: new Uint8Array()
          })) {
            // Should not reach here
          }
        })()
      ).rejects.toThrow()
    })
  })
})
