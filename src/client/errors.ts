/**
 * Base error class for all Flight client errors.
 */
export class FlightError extends Error {
  /** Error name for identification. */
  override readonly name: string = "FlightError"
  /** Original error that caused this error, if any. */
  override readonly cause: unknown

  constructor(message: string, cause?: unknown) {
    super(message, { cause })
    this.cause = cause
  }

  /**
   * Type guard to check if an error is a FlightError.
   */
  static isError(error: unknown): error is FlightError {
    return error instanceof FlightError
  }
}

/**
 * Error thrown when a connection to the Flight server fails.
 */
export class FlightConnectionError extends FlightError {
  /** Error name for identification. */
  override readonly name: string = "FlightConnectionError"
  /** URL that failed to connect. */
  readonly url: string

  constructor(message: string, url: string, cause?: unknown) {
    super(message, cause)
    this.url = url
  }

  /**
   * Type guard to check if an error is a FlightConnectionError.
   */
  static override isError(error: unknown): error is FlightConnectionError {
    return error instanceof FlightConnectionError
  }
}

/**
 * Error thrown when authentication fails.
 */
export class FlightAuthError extends FlightError {
  /** Error name for identification. */
  override readonly name: string = "FlightAuthError"

  /**
   * Type guard to check if an error is a FlightAuthError.
   */
  static override isError(error: unknown): error is FlightAuthError {
    return error instanceof FlightAuthError
  }
}

/**
 * Error thrown when a request times out.
 */
export class FlightTimeoutError extends FlightError {
  /** Error name for identification. */
  override readonly name: string = "FlightTimeoutError"
  /** Timeout duration in milliseconds that was exceeded. */
  readonly timeoutMs: number

  constructor(message: string, timeoutMs: number, cause?: unknown) {
    super(message, cause)
    this.timeoutMs = timeoutMs
  }

  /**
   * Type guard to check if an error is a FlightTimeoutError.
   */
  static override isError(error: unknown): error is FlightTimeoutError {
    return error instanceof FlightTimeoutError
  }
}

/**
 * Error thrown when the server returns an error response.
 */
export class FlightServerError extends FlightError {
  /** Error name for identification. */
  override readonly name: string = "FlightServerError"
  /** gRPC status code from the server. */
  readonly code: string
  /** Additional error details from the server, if provided. */
  readonly details: string | undefined

  constructor(message: string, code: string, details?: string, cause?: unknown) {
    super(message, cause)
    this.code = code
    this.details = details
  }

  /**
   * Type guard to check if an error is a FlightServerError.
   */
  static override isError(error: unknown): error is FlightServerError {
    return error instanceof FlightServerError
  }
}

/**
 * Error thrown when an operation is cancelled.
 */
export class FlightCancelledError extends FlightError {
  /** Error name for identification. */
  override readonly name: string = "FlightCancelledError"

  /**
   * Type guard to check if an error is a FlightCancelledError.
   */
  static override isError(error: unknown): error is FlightCancelledError {
    return error instanceof FlightCancelledError
  }
}
