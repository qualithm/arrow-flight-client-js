import { describe, expect, it } from "vitest"

import {
  FlightAuthError,
  FlightCancelledError,
  FlightConnectionError,
  FlightError,
  FlightServerError,
  FlightTimeoutError
} from "../../../client/errors.js"

describe("FlightError", () => {
  it("creates error with message", () => {
    const error = new FlightError("test message")

    expect(error.message).toBe("test message")
    expect(error.name).toBe("FlightError")
    expect(error.cause).toBeUndefined()
  })

  it("creates error with message and cause", () => {
    const cause = new Error("root cause")
    const error = new FlightError("wrapped error", cause)

    expect(error.message).toBe("wrapped error")
    expect(error.cause).toBe(cause)
  })

  it("is an instance of Error", () => {
    const error = new FlightError("test")

    expect(error).toBeInstanceOf(Error)
    expect(error).toBeInstanceOf(FlightError)
  })

  describe("isError", () => {
    it("returns true for FlightError instances", () => {
      const error = new FlightError("test")

      expect(FlightError.isError(error)).toBe(true)
    })

    it("returns true for FlightError subclasses", () => {
      const authError = new FlightAuthError("auth failed")
      const connError = new FlightConnectionError("conn failed", "http://test")

      expect(FlightError.isError(authError)).toBe(true)
      expect(FlightError.isError(connError)).toBe(true)
    })

    it("returns false for non-FlightError errors", () => {
      expect(FlightError.isError(new Error("regular error"))).toBe(false)
      expect(FlightError.isError(new TypeError("type error"))).toBe(false)
    })

    it("returns false for non-error values", () => {
      expect(FlightError.isError(null)).toBe(false)
      expect(FlightError.isError(undefined)).toBe(false)
      expect(FlightError.isError("error string")).toBe(false)
      expect(FlightError.isError({ message: "fake error" })).toBe(false)
    })
  })
})

describe("FlightConnectionError", () => {
  it("creates error with message and url", () => {
    const error = new FlightConnectionError("connection refused", "http://localhost:8815")

    expect(error.message).toBe("connection refused")
    expect(error.name).toBe("FlightConnectionError")
    expect(error.url).toBe("http://localhost:8815")
    expect(error.cause).toBeUndefined()
  })

  it("creates error with cause", () => {
    const cause = new Error("ECONNREFUSED")
    const error = new FlightConnectionError("connection refused", "http://localhost:8815", cause)

    expect(error.cause).toBe(cause)
  })

  it("is an instance of FlightError", () => {
    const error = new FlightConnectionError("test", "http://test")

    expect(error).toBeInstanceOf(Error)
    expect(error).toBeInstanceOf(FlightError)
    expect(error).toBeInstanceOf(FlightConnectionError)
  })

  describe("isError", () => {
    it("returns true for FlightConnectionError instances", () => {
      const error = new FlightConnectionError("test", "http://test")

      expect(FlightConnectionError.isError(error)).toBe(true)
    })

    it("returns false for other FlightError subclasses", () => {
      const authError = new FlightAuthError("auth failed")

      expect(FlightConnectionError.isError(authError)).toBe(false)
    })

    it("returns false for base FlightError", () => {
      const error = new FlightError("test")

      expect(FlightConnectionError.isError(error)).toBe(false)
    })
  })
})

describe("FlightAuthError", () => {
  it("creates error with message", () => {
    const error = new FlightAuthError("unauthorised")

    expect(error.message).toBe("unauthorised")
    expect(error.name).toBe("FlightAuthError")
  })

  it("creates error with cause", () => {
    const cause = new Error("token expired")
    const error = new FlightAuthError("unauthorised", cause)

    expect(error.cause).toBe(cause)
  })

  it("is an instance of FlightError", () => {
    const error = new FlightAuthError("test")

    expect(error).toBeInstanceOf(FlightError)
    expect(error).toBeInstanceOf(FlightAuthError)
  })

  describe("isError", () => {
    it("returns true for FlightAuthError instances", () => {
      expect(FlightAuthError.isError(new FlightAuthError("test"))).toBe(true)
    })

    it("returns false for other error types", () => {
      expect(FlightAuthError.isError(new FlightError("test"))).toBe(false)
      expect(FlightAuthError.isError(new FlightConnectionError("test", "url"))).toBe(false)
    })
  })
})

describe("FlightTimeoutError", () => {
  it("creates error with message and timeout", () => {
    const error = new FlightTimeoutError("request timed out", 30000)

    expect(error.message).toBe("request timed out")
    expect(error.name).toBe("FlightTimeoutError")
    expect(error.timeoutMs).toBe(30000)
    expect(error.cause).toBeUndefined()
  })

  it("creates error with cause", () => {
    const cause = new Error("deadline exceeded")
    const error = new FlightTimeoutError("timed out", 5000, cause)

    expect(error.cause).toBe(cause)
    expect(error.timeoutMs).toBe(5000)
  })

  it("is an instance of FlightError", () => {
    const error = new FlightTimeoutError("test", 1000)

    expect(error).toBeInstanceOf(FlightError)
    expect(error).toBeInstanceOf(FlightTimeoutError)
  })

  describe("isError", () => {
    it("returns true for FlightTimeoutError instances", () => {
      expect(FlightTimeoutError.isError(new FlightTimeoutError("test", 1000))).toBe(true)
    })

    it("returns false for other error types", () => {
      expect(FlightTimeoutError.isError(new FlightError("test"))).toBe(false)
      expect(FlightTimeoutError.isError(new FlightAuthError("test"))).toBe(false)
    })
  })
})

describe("FlightServerError", () => {
  it("creates error with message and code", () => {
    const error = new FlightServerError("not found", "NOT_FOUND")

    expect(error.message).toBe("not found")
    expect(error.name).toBe("FlightServerError")
    expect(error.code).toBe("NOT_FOUND")
    expect(error.details).toBeUndefined()
    expect(error.cause).toBeUndefined()
  })

  it("creates error with details", () => {
    const error = new FlightServerError(
      "invalid argument",
      "INVALID_ARGUMENT",
      "field X is required"
    )

    expect(error.code).toBe("INVALID_ARGUMENT")
    expect(error.details).toBe("field X is required")
  })

  it("creates error with cause", () => {
    const cause = new Error("grpc error")
    const error = new FlightServerError("server error", "INTERNAL", undefined, cause)

    expect(error.cause).toBe(cause)
  })

  it("is an instance of FlightError", () => {
    const error = new FlightServerError("test", "TEST")

    expect(error).toBeInstanceOf(FlightError)
    expect(error).toBeInstanceOf(FlightServerError)
  })

  describe("isError", () => {
    it("returns true for FlightServerError instances", () => {
      expect(FlightServerError.isError(new FlightServerError("test", "TEST"))).toBe(true)
    })

    it("returns false for other error types", () => {
      expect(FlightServerError.isError(new FlightError("test"))).toBe(false)
      expect(FlightServerError.isError(new FlightConnectionError("test", "url"))).toBe(false)
    })
  })
})

describe("FlightCancelledError", () => {
  it("creates error with message", () => {
    const error = new FlightCancelledError("operation cancelled")

    expect(error.message).toBe("operation cancelled")
    expect(error.name).toBe("FlightCancelledError")
  })

  it("creates error with cause", () => {
    const cause = new Error("user cancelled")
    const error = new FlightCancelledError("cancelled", cause)

    expect(error.cause).toBe(cause)
  })

  it("is an instance of FlightError", () => {
    const error = new FlightCancelledError("test")

    expect(error).toBeInstanceOf(FlightError)
    expect(error).toBeInstanceOf(FlightCancelledError)
  })

  describe("isError", () => {
    it("returns true for FlightCancelledError instances", () => {
      expect(FlightCancelledError.isError(new FlightCancelledError("test"))).toBe(true)
    })

    it("returns false for other error types", () => {
      expect(FlightCancelledError.isError(new FlightError("test"))).toBe(false)
      expect(FlightCancelledError.isError(new FlightTimeoutError("test", 1000))).toBe(false)
    })
  })
})
