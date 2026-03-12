/**
 * Tests for testing/streams.ts
 */
import { describe, expect, it } from "vitest"

import {
  asyncIterable,
  concatStreams,
  delayedIterable,
  emptyStream,
  errorAfter
} from "../../../testing/streams"

describe("testing/streams", () => {
  describe("asyncIterable", () => {
    it("yields all items from array", async () => {
      const items = [1, 2, 3]
      const result: number[] = []

      for await (const item of asyncIterable(items)) {
        result.push(item)
      }

      expect(result).toEqual([1, 2, 3])
    })

    it("yields items in order", async () => {
      const items = ["a", "b", "c"]
      const result: string[] = []

      for await (const item of asyncIterable(items)) {
        result.push(item)
      }

      expect(result).toEqual(["a", "b", "c"])
    })

    it("handles empty array", async () => {
      const result: unknown[] = []

      for await (const item of asyncIterable([])) {
        result.push(item)
      }

      expect(result).toEqual([])
    })
  })

  describe("emptyStream", () => {
    it("yields nothing", async () => {
      const result: unknown[] = []

      for await (const item of emptyStream()) {
        result.push(item)
      }

      expect(result).toEqual([])
    })
  })

  describe("concatStreams", () => {
    it("concatenates two streams", async () => {
      const stream1 = asyncIterable([1, 2])
      const stream2 = asyncIterable([3, 4])
      const result: number[] = []

      for await (const item of concatStreams(stream1, stream2)) {
        result.push(item)
      }

      expect(result).toEqual([1, 2, 3, 4])
    })

    it("concatenates multiple streams", async () => {
      const stream1 = asyncIterable([1])
      const stream2 = asyncIterable([2])
      const stream3 = asyncIterable([3])
      const result: number[] = []

      for await (const item of concatStreams(stream1, stream2, stream3)) {
        result.push(item)
      }

      expect(result).toEqual([1, 2, 3])
    })

    it("handles empty streams", async () => {
      const stream1 = asyncIterable([1])
      const stream2 = emptyStream<number>()
      const stream3 = asyncIterable([2])
      const result: number[] = []

      for await (const item of concatStreams(stream1, stream2, stream3)) {
        result.push(item)
      }

      expect(result).toEqual([1, 2])
    })

    it("handles no streams", async () => {
      const result: unknown[] = []

      for await (const item of concatStreams()) {
        result.push(item)
      }

      expect(result).toEqual([])
    })
  })

  describe("delayedIterable", () => {
    it("yields all items", async () => {
      const items = [1, 2, 3]
      const result: number[] = []

      for await (const item of delayedIterable(items, 1)) {
        result.push(item)
      }

      expect(result).toEqual([1, 2, 3])
    })

    it("introduces delay between items", async () => {
      const items = [1, 2]
      const start = Date.now()

      const result: number[] = []
      for await (const item of delayedIterable(items, 20)) {
        result.push(item)
      }

      const elapsed = Date.now() - start
      // Should take at least 20ms (one delay between two items)
      expect(elapsed).toBeGreaterThanOrEqual(15) // Allow small margin
    })

    it("handles empty array", async () => {
      const result: unknown[] = []

      for await (const item of delayedIterable([], 100)) {
        result.push(item)
      }

      expect(result).toEqual([])
    })
  })

  describe("errorAfter", () => {
    it("yields items before throwing", async () => {
      const items = [1, 2]
      const result: number[] = []

      await expect(
        (async () => {
          for await (const item of errorAfter(items, new Error("test"))) {
            result.push(item)
          }
        })()
      ).rejects.toThrow("test")

      expect(result).toEqual([1, 2])
    })

    it("throws the provided error", async () => {
      const error = new Error("custom error")

      await expect(
        (async () => {
          for await (const _ of errorAfter([], error)) {
            // Won't execute
          }
        })()
      ).rejects.toThrow("custom error")
    })

    it("handles empty items array", async () => {
      const result: unknown[] = []

      await expect(
        (async () => {
          for await (const item of errorAfter([], new Error("error"))) {
            result.push(item)
          }
        })()
      ).rejects.toThrow()

      expect(result).toEqual([])
    })
  })
})
