/**
 * Tests for testing/descriptors.ts
 */
import { describe, expect, it } from "vitest"

import { cmdDescriptor, pathDescriptor } from "../../../testing/descriptors"

describe("testing/descriptors", () => {
  describe("pathDescriptor", () => {
    it("creates path descriptor with single segment", () => {
      const descriptor = pathDescriptor("flights")

      expect(descriptor).toEqual({
        type: "path",
        path: ["flights"]
      })
    })

    it("creates path descriptor with multiple segments", () => {
      const descriptor = pathDescriptor("test", "integers")

      expect(descriptor).toEqual({
        type: "path",
        path: ["test", "integers"]
      })
    })

    it("creates path descriptor with many segments", () => {
      const descriptor = pathDescriptor("db", "schema", "table", "partition")

      expect(descriptor).toEqual({
        type: "path",
        path: ["db", "schema", "table", "partition"]
      })
    })

    it("handles empty path", () => {
      const descriptor = pathDescriptor()

      expect(descriptor).toEqual({
        type: "path",
        path: []
      })
    })
  })

  describe("cmdDescriptor", () => {
    it("creates command descriptor from string", () => {
      const descriptor = cmdDescriptor("SELECT * FROM users")

      expect(descriptor.type).toBe("cmd")
      if (descriptor.type === "cmd") {
        expect(descriptor.cmd).toBeInstanceOf(Uint8Array)
      }
    })

    it("encodes string as UTF-8", () => {
      const descriptor = cmdDescriptor("hello")

      if (descriptor.type === "cmd") {
        expect(Array.from(descriptor.cmd)).toEqual([104, 101, 108, 108, 111])
      }
    })

    it("creates command descriptor from Uint8Array", () => {
      const bytes = new Uint8Array([0x01, 0x02, 0x03])
      const descriptor = cmdDescriptor(bytes)

      expect(descriptor).toEqual({
        type: "cmd",
        cmd: bytes
      })
    })

    it("preserves binary data", () => {
      const bytes = new Uint8Array([255, 0, 128])
      const descriptor = cmdDescriptor(bytes)

      if (descriptor.type === "cmd") {
        expect(Array.from(descriptor.cmd)).toEqual([255, 0, 128])
      }
    })

    it("handles empty string", () => {
      const descriptor = cmdDescriptor("")

      expect(descriptor.type).toBe("cmd")
      if (descriptor.type === "cmd") {
        expect(descriptor.cmd.length).toBe(0)
      }
    })

    it("handles empty Uint8Array", () => {
      const descriptor = cmdDescriptor(new Uint8Array(0))

      expect(descriptor.type).toBe("cmd")
      if (descriptor.type === "cmd") {
        expect(descriptor.cmd.length).toBe(0)
      }
    })
  })
})
