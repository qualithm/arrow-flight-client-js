#!/usr/bin/env bun
/**
 * Cross-runtime validation script.
 *
 * Tests that the library can be imported and used correctly across
 * different JavaScript runtimes (Bun, Node.js, Deno).
 *
 * Run with: bun run scripts/validate-runtime.ts
 */

import { spawn } from "node:child_process"
import { mkdtemp, rm, writeFile } from "node:fs/promises"
import { tmpdir } from "node:os"
import { join } from "node:path"

// ── Test Code ─────────────────────────────────────────────────────────

function getTestCode(importPath: string): string {
  return `
import {
  createFlightClient,
  createFlightSqlClient,
  FlightClient,
  FlightSqlClient,
  FlightError,
  FlightConnectionError,
  FlightAuthError,
  FlightServerError,
  FlightTimeoutError,
  FlightCancelledError,
  DEFAULT_TIMEOUT_MS,
  encodeTableToFlightData,
  decodeFlightDataToTable,
} from "${importPath}";

// Verify exports exist
const checks = [
  ["createFlightClient", typeof createFlightClient === "function"],
  ["createFlightSqlClient", typeof createFlightSqlClient === "function"],
  ["FlightClient", typeof FlightClient === "function"],
  ["FlightSqlClient", typeof FlightSqlClient === "function"],
  ["FlightError", typeof FlightError === "function"],
  ["FlightConnectionError", typeof FlightConnectionError === "function"],
  ["FlightAuthError", typeof FlightAuthError === "function"],
  ["FlightServerError", typeof FlightServerError === "function"],
  ["FlightTimeoutError", typeof FlightTimeoutError === "function"],
  ["FlightCancelledError", typeof FlightCancelledError === "function"],
  ["DEFAULT_TIMEOUT_MS", typeof DEFAULT_TIMEOUT_MS === "number"],
  ["encodeTableToFlightData", typeof encodeTableToFlightData === "function"],
  ["decodeFlightDataToTable", typeof decodeFlightDataToTable === "function"],
];

let passed = 0;
let failed = 0;

for (const [name, ok] of checks) {
  if (ok) {
    passed++;
  } else {
    failed++;
    console.error(\`FAIL: \${name} not exported correctly\`);
  }
}

// Test client creation (without connecting)
try {
  const client = createFlightClient({ url: "http://localhost:50051" });
  client.close();
  passed++;
} catch (error) {
  failed++;
  console.error("FAIL: createFlightClient threw:", error);
}

try {
  const client = createFlightSqlClient({ url: "http://localhost:50051" });
  client.close();
  passed++;
} catch (error) {
  failed++;
  console.error("FAIL: createFlightSqlClient threw:", error);
}

// Test error type guards
try {
  const error = new FlightError("test error");
  if (!FlightError.isError(error)) {
    throw new Error("FlightError.isError failed");
  }
  passed++;
} catch (error) {
  failed++;
  console.error("FAIL: FlightError.isError threw:", error);
}

console.log(\`Passed: \${passed}, Failed: \${failed}\`);
process.exit(failed > 0 ? 1 : 0);
`.trim()
}

// ── Runtime Detection ─────────────────────────────────────────────────

type RuntimeInfo = {
  name: string
  command: string
  args: string[]
  available: boolean
  version?: string
}

async function checkRuntime(
  name: string,
  command: string,
  versionArg: string
): Promise<RuntimeInfo> {
  return new Promise((resolve) => {
    const proc = spawn(command, [versionArg], { stdio: ["ignore", "pipe", "ignore"] })
    let version = ""

    proc.stdout.on("data", (data: Buffer) => {
      version += data.toString()
    })

    proc.on("error", () => {
      resolve({ name, command, args: [], available: false })
    })

    proc.on("close", (code) => {
      resolve({
        name,
        command,
        args: [],
        available: code === 0,
        version: version.trim().split("\n")[0]
      })
    })
  })
}

// ── Test Runner ───────────────────────────────────────────────────────

async function runTest(
  runtime: RuntimeInfo,
  testFile: string,
  testDir: string,
  importMapPath: string
): Promise<{ success: boolean; output: string }> {
  return new Promise((resolve) => {
    let args = [...runtime.args]
    // Add import map for Deno
    if (runtime.name === "Deno") {
      args = ["run", "--allow-read", "--allow-env", "--allow-net", `--import-map=${importMapPath}`]
    }
    args.push(testFile)

    const proc = spawn(runtime.command, args, { cwd: testDir, stdio: ["ignore", "pipe", "pipe"] })
    let output = ""

    proc.stdout.on("data", (data: Buffer) => {
      output += data.toString()
    })
    proc.stderr.on("data", (data: Buffer) => {
      output += data.toString()
    })

    proc.on("error", (error) => {
      resolve({ success: false, output: error.message })
    })

    proc.on("close", (code) => {
      resolve({ success: code === 0, output: output.trim() })
    })
  })
}

// ── Main ──────────────────────────────────────────────────────────────

async function main(): Promise<void> {
  console.log("Cross-Runtime Validation")
  console.log("========================\n")

  // Check available runtimes
  const runtimes: RuntimeInfo[] = await Promise.all([
    checkRuntime("Bun", "bun", "--version").then((r) => ({ ...r, args: ["run"] })),
    checkRuntime("Node.js", "node", "--version").then((r) => ({
      ...r,
      args: ["--experimental-vm-modules"]
    })),
    checkRuntime("Deno", "deno", "--version").then((r) => ({
      ...r,
      args: ["run", "--allow-read", "--allow-env", "--allow-net", "--node-modules-dir=auto"]
    }))
  ])

  console.log("Available runtimes:")
  for (const runtime of runtimes) {
    const status = runtime.available ? `✓ ${runtime.version ?? "unknown"}` : "✗ not found"
    console.log(`  ${runtime.name}: ${status}`)
  }
  console.log()

  const available = runtimes.filter((r) => r.available)
  if (available.length === 0) {
    console.error("No runtimes available for testing")
    process.exit(1)
  }

  // Create temporary test directory
  const tmpDir = await mkdtemp(join(tmpdir(), "arrow-flight-test-"))
  const distPath = join(process.cwd(), "dist", "index.js")

  try {
    // Write test file that imports directly from dist using absolute path
    const testFile = join(tmpDir, "test.mjs")
    await writeFile(testFile, getTestCode(distPath))

    // Create import map for Deno
    const importMapPath = join(tmpDir, "import_map.json")
    await writeFile(
      importMapPath,
      JSON.stringify({
        imports: {
          "@connectrpc/connect-node": "npm:@connectrpc/connect-node",
          "@connectrpc/connect": "npm:@connectrpc/connect",
          "@bufbuild/protobuf": "npm:@bufbuild/protobuf",
          "@bufbuild/protobuf/wkt": "npm:@bufbuild/protobuf/wkt",
          "@bufbuild/protobuf/codegenv2": "npm:@bufbuild/protobuf/codegenv2",
          "apache-arrow": "npm:apache-arrow"
        }
      })
    )

    // Run tests
    console.log("Running validation tests:")
    console.log("-".repeat(40))

    let passed = 0
    let failed = 0

    for (const runtime of available) {
      process.stdout.write(`${runtime.name}: `)
      const result = await runTest(runtime, testFile, tmpDir, importMapPath)

      if (result.success) {
        console.log("✓ PASS")
        passed++
      } else {
        console.log("✗ FAIL")
        console.log(`  Output: ${result.output}`)
        failed++
      }
    }

    console.log("-".repeat(40))
    console.log()
    console.log(`Results: ${String(passed)} passed, ${String(failed)} failed`)
    console.log()

    if (failed > 0) {
      process.exit(1)
    }
  } finally {
    // Cleanup
    await rm(tmpDir, { recursive: true, force: true })
  }
}

main().catch((error: unknown) => {
  console.error("Validation failed:", error)
  process.exit(1)
})
