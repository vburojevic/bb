// Behavioral tests for the dynamic-tool MCP server: the stdio JSON-RPC front
// an ACP agent talks to. The "bridge" behind it is a real TCP listener under
// the test's control, so the request/response contract is exercised exactly
// as the bridge process implements it.

import { createServer, type Server, type Socket } from "node:net";
import { afterEach, describe, expect, it } from "vitest";

import {
  handleMcpRequest,
  type McpIo,
  type McpServerEnvironment,
} from "./tool-proxy-mcp.js";

const TOOL = {
  name: "wait",
  description: "Waits, then answers.",
  inputSchema: { type: "object", properties: {} },
};

interface CapturedCall {
  arguments: Record<string, unknown>;
  callId: string;
  threadId: string;
  token: string;
  tool: string;
}

/** A one-shot fake bridge: reads one request line, delays, writes one line. */
function startFakeBridge(options: {
  delayMs: number;
  response: (request: CapturedCall) => unknown;
}): Promise<{ server: Server; port: number; calls: CapturedCall[] }> {
  const calls: CapturedCall[] = [];
  const server = createServer((socket: Socket) => {
    let buffer = "";
    socket.setEncoding("utf8");
    socket.on("data", (chunk) => {
      buffer += chunk;
      const newline = buffer.indexOf("\n");
      if (newline === -1) return;
      const request = JSON.parse(buffer.slice(0, newline)) as CapturedCall;
      calls.push(request);
      setTimeout(() => {
        socket.end(`${JSON.stringify(options.response(request))}\n`);
      }, options.delayMs);
    });
  });
  return new Promise((resolveServer) => {
    server.listen(0, "127.0.0.1", () => {
      const address = server.address();
      if (address === null || typeof address === "string") {
        throw new Error("fake bridge did not bind");
      }
      resolveServer({ server, port: address.port, calls });
    });
  });
}

function collectIo(): { io: McpIo; messages: Record<string, any>[] } {
  const messages: Record<string, any>[] = [];
  return {
    io: { write: (message) => messages.push(message as Record<string, any>) },
    messages,
  };
}

function envFor(port: number): McpServerEnvironment {
  return {
    host: "127.0.0.1",
    port,
    threadId: "thread-1",
    token: "secret",
    tools: [TOOL],
  };
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

describe("dynamic-tool MCP server", () => {
  const servers: Server[] = [];
  afterEach(async () => {
    await Promise.all(
      servers.splice(0).map(
        (server) => new Promise((resolve) => server.close(resolve)),
      ),
    );
  });

  async function withBridge(options: {
    delayMs: number;
    response: (request: CapturedCall) => unknown;
  }) {
    const bridge = await startFakeBridge(options);
    servers.push(bridge.server);
    return bridge;
  }

  it("routes a tools/call to the bridge with env identity and returns the content", async () => {
    const bridge = await withBridge({
      delayMs: 0,
      response: () => ({ ok: true, content: "bridge says hi" }),
    });
    const { io, messages } = collectIo();
    await handleMcpRequest(
      envFor(bridge.port),
      {
        id: 1,
        method: "tools/call",
        params: { name: "wait", arguments: { question: "?" } },
      },
      io,
    );

    expect(bridge.calls).toHaveLength(1);
    const call = bridge.calls[0]!;
    expect(call.tool).toBe("wait");
    expect(call.arguments).toEqual({ question: "?" });
    expect(call.threadId).toBe("thread-1");
    expect(call.token).toBe("secret");
    expect(call.callId).toContain("acp-mcp-wait-");

    expect(messages).toHaveLength(1);
    expect(messages[0]).toEqual({
      jsonrpc: "2.0",
      id: 1,
      result: { content: [{ type: "text", text: "bridge says hi" }] },
    });
  });

  it("emits progress notifications while a call pends, and stops with the result", async () => {
    const bridge = await withBridge({
      delayMs: 300,
      response: () => ({ ok: true, content: "answered" }),
    });
    const { io, messages } = collectIo();
    await handleMcpRequest(
      envFor(bridge.port),
      {
        id: 7,
        method: "tools/call",
        params: {
          name: "wait",
          arguments: {},
          _meta: { progressToken: "tok-1" },
        },
      },
      io,
      { progressIntervalMs: 40 },
    );

    const progress = messages.filter(
      (message) => message.method === "notifications/progress",
    );
    // 300ms at a 40ms drip: several keepalives, all carrying the client's token.
    expect(progress.length).toBeGreaterThanOrEqual(3);
    for (const notification of progress) {
      expect(notification.params.progressToken).toBe("tok-1");
      expect(typeof notification.params.progress).toBe("number");
    }
    // Every keepalive preceded the result — the client never saw a quiet 60s.
    const resultIndex = messages.findIndex((message) => message.id === 7);
    expect(resultIndex).toBeGreaterThan(0);
    expect(messages[resultIndex - 1]?.method).toBe("notifications/progress");

    // The drip stopped with the result: nothing more arrives afterwards.
    const settledCount = messages.length;
    await sleep(150);
    expect(messages).toHaveLength(settledCount);
  });

  it("stays silent for the whole call when no progressToken was sent", async () => {
    const bridge = await withBridge({
      delayMs: 120,
      response: () => ({ ok: true, content: "quiet" }),
    });
    const { io, messages } = collectIo();
    await handleMcpRequest(
      envFor(bridge.port),
      { id: 3, method: "tools/call", params: { name: "wait", arguments: {} } },
      io,
      { progressIntervalMs: 30 },
    );
    expect(messages).toHaveLength(1);
    expect(messages[0]?.id).toBe(3);
  });

  it("answers an unknown tool without touching the bridge", async () => {
    const bridge = await withBridge({
      delayMs: 0,
      response: () => ({ ok: true, content: "" }),
    });
    const { io, messages } = collectIo();
    await handleMcpRequest(
      envFor(bridge.port),
      { id: 4, method: "tools/call", params: { name: "nope", arguments: {} } },
      io,
    );
    expect(bridge.calls).toHaveLength(0);
    expect(messages[0]?.error).toMatchObject({ code: -32602 });
  });

  it("forwards a client cancellation to the bridge and settles the call locally", async () => {
    // A fake bridge that reads EVERY line (request + control frames) and never
    // responds on its own — the cancel path must not wait for it.
    const frames: Record<string, any>[] = [];
    const server = createServer((socket) => {
      let buffer = "";
      socket.setEncoding("utf8");
      socket.on("data", (chunk) => {
        buffer += chunk;
        let newline;
        while ((newline = buffer.indexOf("\n")) !== -1) {
          frames.push(JSON.parse(buffer.slice(0, newline)) as Record<string, any>);
          buffer = buffer.slice(newline + 1);
        }
      });
    });
    servers.push(server);
    const port = await new Promise<number>((resolvePort) => {
      server.listen(0, "127.0.0.1", () => {
        const address = server.address();
        if (address === null || typeof address === "string") {
          throw new Error("fake bridge did not bind");
        }
        resolvePort(address.port);
      });
    });

    const { io, messages } = collectIo();
    const pending = handleMcpRequest(
      envFor(port),
      { id: 9, method: "tools/call", params: { name: "wait", arguments: {} } },
      io,
    );
    // Let the request land, then abandon it the way an MCP client does.
    await expect.poll(() => frames.length, { timeout: 2_000 }).toBe(1);
    await handleMcpRequest(
      envFor(port),
      {
        method: "notifications/cancelled",
        params: { requestId: 9, reason: "MCP error -32001: Request timed out" },
      },
      io,
    );
    await pending;

    // The bridge learned WHICH call to unwind, on the same connection. The
    // frame flushes a tick after the local settle, so poll for it.
    await expect.poll(() => frames.length, { timeout: 2_000 }).toBe(2);
    expect(frames[0]!.tool).toBe("wait");
    expect(frames[1]).toEqual({ cancel: frames[0]!.callId });

    // And the caller settled immediately with an error result, not a hang.
    expect(messages).toHaveLength(1);
    expect(messages[0]).toMatchObject({
      id: 9,
      result: {
        isError: true,
        content: [{ type: "text", text: expect.stringContaining("cancelled") }],
      },
    });
  });
});
