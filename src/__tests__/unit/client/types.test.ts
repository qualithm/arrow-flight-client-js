import { describe, expect, it } from "vitest"

import { DEFAULT_TIMEOUT_MS, resolveOptions } from "../../../client/types.js"

describe("DEFAULT_TIMEOUT_MS", () => {
  it("is 30 seconds", () => {
    expect(DEFAULT_TIMEOUT_MS).toBe(30_000)
  })
})

describe("resolveOptions", () => {
  it("returns url unchanged", () => {
    const result = resolveOptions({ url: "https://flight.example.com:8815" })

    expect(result.url).toBe("https://flight.example.com:8815")
  })

  it("applies default timeout when not provided", () => {
    const result = resolveOptions({ url: "https://example.com" })

    expect(result.timeoutMs).toBe(DEFAULT_TIMEOUT_MS)
  })

  it("uses provided timeout when specified", () => {
    const result = resolveOptions({
      url: "https://example.com",
      timeoutMs: 5000
    })

    expect(result.timeoutMs).toBe(5000)
  })

  it("preserves headers when provided", () => {
    const headers = { Authorization: "Bearer token123", "X-Custom": "value" }
    const result = resolveOptions({
      url: "https://example.com",
      headers
    })

    expect(result.headers).toEqual(headers)
  })

  it("leaves headers undefined when not provided", () => {
    const result = resolveOptions({ url: "https://example.com" })

    expect(result.headers).toBeUndefined()
  })

  it("handles all options together", () => {
    const result = resolveOptions({
      url: "https://flight.example.com:8815",
      headers: { Authorization: "Bearer xyz" },
      timeoutMs: 60000
    })

    expect(result).toEqual({
      url: "https://flight.example.com:8815",
      headers: { Authorization: "Bearer xyz" },
      timeoutMs: 60000
    })
  })

  it("accepts zero timeout", () => {
    const result = resolveOptions({
      url: "https://example.com",
      timeoutMs: 0
    })

    // 0 is falsy but should still be used (not replaced with default)
    // The nullish coalescing operator (??) should handle this correctly
    expect(result.timeoutMs).toBe(0)
  })
})
