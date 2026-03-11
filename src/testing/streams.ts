/**
 * Mock stream utilities for testing stream consumers.
 *
 * @packageDocumentation
 */

/**
 * Create an async iterable from an array of items.
 *
 * Useful for testing stream consumers with predetermined data.
 *
 * @param items - Array of items to yield
 * @returns Async iterable that yields each item
 *
 * @example
 * ```ts
 * const flightData = [createEmptyFlightData()]
 * const stream = asyncIterable(flightData)
 *
 * for await (const data of stream) {
 *   console.log(data)
 * }
 * ```
 */
// eslint-disable-next-line @typescript-eslint/require-await
export async function* asyncIterable<T>(items: T[]): AsyncIterable<T> {
  for (const item of items) {
    yield item
  }
}

/**
 * Create an empty async iterable.
 *
 * Useful for testing edge cases with empty streams.
 *
 * @returns Empty async iterable
 *
 * @example
 * ```ts
 * const stream = emptyStream()
 * const result = await collectFlightData(stream)
 * expect(result).toEqual([])
 * ```
 */

export async function* emptyStream<T>(): AsyncIterable<T> {
  // Yields nothing
}

/**
 * Concatenate multiple async iterables into one.
 *
 * @param streams - Async iterables to concatenate
 * @returns Combined async iterable
 *
 * @example
 * ```ts
 * const stream1 = asyncIterable([data1])
 * const stream2 = asyncIterable([data2, data3])
 * const combined = concatStreams(stream1, stream2)
 *
 * // Yields data1, data2, data3 in order
 * ```
 */
export async function* concatStreams<T>(...streams: AsyncIterable<T>[]): AsyncIterable<T> {
  for (const stream of streams) {
    for await (const item of stream) {
      yield item
    }
  }
}

/**
 * Create an async iterable that yields items with delays.
 *
 * Useful for testing timeout handling and streaming behavior.
 *
 * @param items - Array of items to yield
 * @param delayMs - Delay between items in milliseconds
 * @returns Async iterable with delays
 *
 * @example
 * ```ts
 * const stream = delayedIterable([data1, data2], 100)
 * // Yields data1, waits 100ms, yields data2
 * ```
 */
export async function* delayedIterable<T>(items: T[], delayMs: number): AsyncIterable<T> {
  for (let i = 0; i < items.length; i++) {
    if (i > 0) {
      await new Promise((resolve) => setTimeout(resolve, delayMs))
    }
    yield items[i]
  }
}

/**
 * Create an async iterable that throws an error after yielding some items.
 *
 * Useful for testing error handling in stream consumers.
 *
 * @param items - Items to yield before throwing
 * @param error - Error to throw
 * @returns Async iterable that throws
 *
 * @example
 * ```ts
 * const stream = errorAfter([data1], new Error("connection lost"))
 * // Yields data1, then throws Error
 * ```
 */
// eslint-disable-next-line @typescript-eslint/require-await
export async function* errorAfter<T>(items: T[], error: Error): AsyncIterable<T> {
  for (const item of items) {
    yield item
  }
  throw error
}
