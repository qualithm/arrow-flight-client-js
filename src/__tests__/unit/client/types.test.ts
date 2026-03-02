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

    expect(result.url).toBe("https://flight.example.com:8815")
    expect(result.headers).toEqual({ Authorization: "Bearer xyz" })
    expect(result.timeoutMs).toBe(60000)
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

  describe("bearer auth", () => {
    it("adds Authorization header for bearer auth", () => {
      const result = resolveOptions({
        url: "https://example.com",
        auth: { type: "bearer", token: "my-token" }
      })

      expect(result.headers).toEqual({ Authorization: "Bearer my-token" })
    })

    it("preserves existing headers with bearer auth", () => {
      const result = resolveOptions({
        url: "https://example.com",
        headers: { "X-Custom": "value" },
        auth: { type: "bearer", token: "my-token" }
      })

      expect(result.headers).toEqual({
        "X-Custom": "value",
        Authorization: "Bearer my-token"
      })
    })

    it("overrides existing Authorization header with bearer auth", () => {
      const result = resolveOptions({
        url: "https://example.com",
        headers: { Authorization: "old-value" },
        auth: { type: "bearer", token: "my-token" }
      })

      expect(result.headers?.Authorization).toBe("Bearer my-token")
    })
  })

  describe("basic auth", () => {
    it("preserves basic auth config without modifying headers", () => {
      const result = resolveOptions({
        url: "https://example.com",
        auth: { type: "basic", credentials: { username: "user", password: "pass" } }
      })

      // Basic auth doesn't add headers immediately - handshake is required
      expect(result.headers).toBeUndefined()
      expect(result.auth).toEqual({
        type: "basic",
        credentials: { username: "user", password: "pass" }
      })
    })
  })

  describe("tls options", () => {
    it("preserves TLS options", () => {
      const result = resolveOptions({
        url: "https://example.com",
        tls: {
          cert: "cert-data",
          key: "key-data",
          ca: "ca-data",
          rejectUnauthorized: true
        }
      })

      expect(result.tls).toEqual({
        cert: "cert-data",
        key: "key-data",
        ca: "ca-data",
        rejectUnauthorized: true
      })
    })
  })

  describe("nodeOptions", () => {
    it("preserves nodeOptions", () => {
      const nodeOptions = { timeout: 5000 }
      const result = resolveOptions({
        url: "https://example.com",
        nodeOptions
      })

      expect(result.nodeOptions).toEqual(nodeOptions)
    })
  })
})
