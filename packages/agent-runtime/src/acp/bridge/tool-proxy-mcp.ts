import { createConnection } from "node:net";
import { createInterface } from "node:readline";
import { dynamicToolSchema, type DynamicTool } from "@bb/domain";
import { z } from "zod";

export const ACP_BRIDGE_MCP_SERVER_NAME = "bb-bridge";

const ENV_HOST = "BB_ACP_DYNAMIC_TOOL_HOST";
const ENV_PORT = "BB_ACP_DYNAMIC_TOOL_PORT";
const ENV_TOKEN = "BB_ACP_DYNAMIC_TOOL_TOKEN";
const ENV_THREAD_ID = "BB_ACP_DYNAMIC_TOOL_THREAD_ID";
const ENV_TOOLS = "BB_ACP_DYNAMIC_TOOLS";

export interface AcpMcpServerConfig {
  name: string;
  command: string;
  args: string[];
  env: { name: string; value: string }[];
}

export interface BuildAcpMcpServerConfigArgs {
  bridgeArgs: string[];
  command: string;
  dynamicTools: readonly DynamicTool[];
  host: string;
  port: number;
  runtimeEnv: { name: string; value: string }[];
  threadId: string;
  token: string;
}

interface BridgeToolCallRequest {
  arguments: Record<string, unknown>;
  callId: string;
  threadId: string;
  token: string;
  tool: string;
}

type BridgeToolCallResponse =
  | { ok: true; content: string; isError?: boolean }
  | { ok: false; error: string };

const bridgeToolCallResponseSchema = z.union([
  z.object({
    ok: z.literal(true),
    content: z.string(),
    isError: z.boolean().optional(),
  }),
  z.object({ ok: z.literal(false), error: z.string() }),
]);

interface JsonRpcMessage {
  id?: string | number;
  method?: string;
  params?: unknown;
}

export interface McpServerEnvironment {
  host: string;
  port: number;
  threadId: string;
  token: string;
  tools: DynamicTool[];
}

let nextMcpToolCallId = 0;

export function buildAcpMcpServerConfig(
  args: BuildAcpMcpServerConfigArgs,
): AcpMcpServerConfig {
  return {
    name: ACP_BRIDGE_MCP_SERVER_NAME,
    command: args.command,
    args: args.bridgeArgs,
    env: [
      ...args.runtimeEnv,
      { name: ENV_HOST, value: args.host },
      { name: ENV_PORT, value: String(args.port) },
      { name: ENV_TOKEN, value: args.token },
      { name: ENV_THREAD_ID, value: args.threadId },
      { name: ENV_TOOLS, value: JSON.stringify(args.dynamicTools) },
    ],
  };
}

function readEnvironment(): McpServerEnvironment {
  const port = Number(process.env[ENV_PORT]);
  if (!Number.isInteger(port) || port <= 0) {
    throw new Error(`${ENV_PORT} must be a positive integer`);
  }
  const host = process.env[ENV_HOST];
  const token = process.env[ENV_TOKEN];
  const threadId = process.env[ENV_THREAD_ID];
  const toolsJson = process.env[ENV_TOOLS];
  if (!host || !token || !threadId || !toolsJson) {
    throw new Error("Missing ACP dynamic tool MCP server environment");
  }
  const parsedTools = JSON.parse(toolsJson) as unknown;
  const tools = dynamicToolSchema.array().parse(parsedTools);
  return {
    host,
    port,
    threadId,
    token,
    tools,
  };
}

/** Outbound side of the MCP stdio channel, injected for tests. */
export interface McpIo {
  write(message: unknown): void;
}

const stdoutIo: McpIo = {
  write: (message) => {
    process.stdout.write(`${JSON.stringify(message)}\n`);
  },
};

function writeResult(io: McpIo, id: string | number, result: unknown): void {
  io.write({ jsonrpc: "2.0", id, result });
}

function writeError(io: McpIo, id: string | number, code: number, message: string): void {
  io.write({ jsonrpc: "2.0", id, error: { code, message } });
}

function mcpToolCallId(toolName: string): string {
  nextMcpToolCallId += 1;
  return `acp-mcp-${toolName}-${Date.now()}-${nextMcpToolCallId}`;
}

/**
 * Tools that wait on the user (AskUserQuestion and friends) can pend for most
 * of an hour, but MCP clients default to a 60s request timeout. The protocol's
 * escape hatch is progress: clients that opt in (`resetTimeoutOnProgress`)
 * reset that timer on every `notifications/progress` carrying their
 * progressToken, so one drip per fraction of the default window keeps a
 * cooperative client's request alive for the whole interaction. Clients that
 * never sent a token (or never opted in) see nothing — notifications to them
 * would be noise.
 */
const PROGRESS_INTERVAL_MS = 15_000;

function startProgressNotifications(
  io: McpIo,
  progressToken: unknown,
  intervalMs = PROGRESS_INTERVAL_MS,
): () => void {
  if (
    typeof progressToken !== "string" &&
    typeof progressToken !== "number"
  ) {
    return () => {};
  }
  const startedAt = Date.now();
  const timer = setInterval(() => {
    io.write({
      jsonrpc: "2.0",
      method: "notifications/progress",
      params: {
        progressToken,
        progress: Date.now() - startedAt,
        message: "waiting for the tool call to finish",
      },
    });
  }, intervalMs);
  // The server must exit when the agent closes stdin, not linger for a timer.
  timer.unref();
  return () => clearInterval(timer);
}

interface CancellableBridgeCall {
  promise: Promise<BridgeToolCallResponse>;
  /** Notifies the bridge (which unwinds the server-side work) and settles locally. */
  cancel: () => void;
}

/**
 * In-flight tool calls by MCP request id. MCP clients that abandon a request
 * (timeout, interrupt) send `notifications/cancelled`; without forwarding that,
 * the server-side work — a question waiting on the user, for instance — keeps
 * pending for its full budget while the agent has long moved on.
 */
const inFlightCalls = new Map<string | number, CancellableBridgeCall>();

function callBridge(
  env: McpServerEnvironment,
  request: Omit<BridgeToolCallRequest, "threadId" | "token">,
): CancellableBridgeCall {
  const socket = createConnection({ host: env.host, port: env.port });
  let buffer = "";
  let rejectPromise: (error: Error) => void = () => {};
  socket.setEncoding("utf8");
  const promise = new Promise<BridgeToolCallResponse>((resolve, reject) => {
    rejectPromise = reject;
    socket.on("connect", () => {
      const payload: BridgeToolCallRequest = {
        ...request,
        threadId: env.threadId,
        token: env.token,
      };
      socket.write(`${JSON.stringify(payload)}\n`);
    });
    socket.on("data", (chunk) => {
      buffer += chunk;
      const newlineIndex = buffer.indexOf("\n");
      if (newlineIndex === -1) {
        return;
      }
      const line = buffer.slice(0, newlineIndex);
      socket.end();
      try {
        resolve(bridgeToolCallResponseSchema.parse(JSON.parse(line)));
      } catch (error) {
        reject(error);
      }
    });
    socket.on("error", reject);
    socket.on("end", () => {
      if (!buffer.includes("\n")) {
        reject(new Error("ACP dynamic tool bridge closed without a response"));
      }
    });
  });
  // A settled promise ignores the loser, so a late cancel after completion is
  // a no-op, and a late completion after cancel never surfaces.
  promise.catch(() => {});
  const cancel = () => {
    try {
      socket.write(`${JSON.stringify({ cancel: request.callId })}\n`);
    } catch {
      // The bridge is already gone; the local settle below still applies.
    }
    socket.end();
    rejectPromise(new Error("Tool call cancelled by the agent's MCP client"));
  };
  return { promise, cancel };
}

function objectParams(params: unknown): Record<string, unknown> {
  return params && typeof params === "object" && !Array.isArray(params)
    ? (params as Record<string, unknown>)
    : {};
}

/** Exported for tests; production wiring is `runAcpDynamicToolMcpServer`. */
export async function handleMcpRequest(
  env: McpServerEnvironment,
  message: JsonRpcMessage,
  io: McpIo,
  options?: { progressIntervalMs?: number },
): Promise<void> {
  if (message.id === undefined) {
    // Notifications need no response. The only one that matters here is the
    // client abandoning a request — forward that so the work behind it stops.
    if (message.method === "notifications/cancelled") {
      const requestId = objectParams(message.params).requestId;
      if (typeof requestId === "string" || typeof requestId === "number") {
        inFlightCalls.get(requestId)?.cancel();
      }
    }
    return;
  }
  if (message.method === undefined) {
    return;
  }

  switch (message.method) {
    case "initialize":
      writeResult(io, message.id, {
        protocolVersion:
          typeof objectParams(message.params).protocolVersion === "string"
            ? objectParams(message.params).protocolVersion
            : "2024-11-05",
        capabilities: { tools: {} },
        serverInfo: { name: ACP_BRIDGE_MCP_SERVER_NAME, version: "1.0.0" },
      });
      return;

    case "tools/list":
      writeResult(io, message.id, {
        tools: env.tools.map((tool) => ({
          name: tool.name,
          description: tool.description,
          inputSchema: tool.inputSchema,
        })),
      });
      return;

    case "tools/call": {
      const params = objectParams(message.params);
      const name = typeof params.name === "string" ? params.name : "";
      const tool = env.tools.find((candidate) => candidate.name === name);
      if (!tool) {
        writeError(io, message.id, -32602, `Unknown tool: ${name}`);
        return;
      }
      const rawArguments = params.arguments;
      const toolArguments =
        rawArguments &&
        typeof rawArguments === "object" &&
        !Array.isArray(rawArguments)
          ? (rawArguments as Record<string, unknown>)
          : {};
      const stopProgress = startProgressNotifications(
        io,
        objectParams(params._meta).progressToken,
        options?.progressIntervalMs,
      );
      const callId = mcpToolCallId(tool.name);
      const call = callBridge(env, {
        arguments: toolArguments,
        callId,
        tool: tool.name,
      });
      inFlightCalls.set(message.id, call);
      try {
        const result = await call.promise;
        if (!result.ok) {
          writeResult(io, message.id, {
            content: [{ type: "text", text: result.error }],
            isError: true,
          });
          return;
        }
        writeResult(io, message.id, {
          content: [{ type: "text", text: result.content }],
          ...(result.isError ? { isError: true } : {}),
        });
      } catch (error) {
        writeResult(io, message.id, {
          content: [
            {
              type: "text",
              text: error instanceof Error ? error.message : String(error),
            },
          ],
          isError: true,
        });
      } finally {
        inFlightCalls.delete(message.id);
        stopProgress();
      }
      return;
    }

    default:
      writeError(
        io,
        message.id,
        -32601,
        `Unsupported MCP method: ${message.method}`,
      );
  }
}

export function runAcpDynamicToolMcpServer(): void {
  const env = readEnvironment();
  const rl = createInterface({ input: process.stdin, terminal: false });
  rl.on("line", (line) => {
    const trimmed = line.trim();
    if (!trimmed) {
      return;
    }
    let message: JsonRpcMessage;
    try {
      message = JSON.parse(trimmed) as JsonRpcMessage;
    } catch {
      return;
    }
    void handleMcpRequest(env, message, stdoutIo);
  });
}
