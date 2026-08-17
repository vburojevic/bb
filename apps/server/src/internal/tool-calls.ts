import {
  hostDaemonToolCallRequestSchema,
  typedRoutes,
  type HostDaemonInternalSchema,
} from "@bb/host-daemon-contract";
import type { ToolCallResponse } from "@bb/domain";
import type { Hono } from "hono";
import type { AppDeps } from "../types.js";
import { ApiError } from "../errors.js";
import { requireThreadEnvironment } from "../services/lib/entity-lookup.js";
import {
  findPluginAgentTool,
  invokePluginAgentTool,
} from "../services/plugins/plugin-agent-contributions.js";
import {
  handleUpdateEnvironmentDirectoryToolCall,
  UPDATE_ENVIRONMENT_DIRECTORY_TOOL_NAME,
} from "../services/threads/thread-environment-directory.js";
import { requireAuthenticatedDaemonSession } from "./session-state.js";

const textEncoder = new TextEncoder();

/**
 * Interactive plugin tools can wait on the user for most of an hour. Every
 * inactivity timer on the daemon→server path must never see the response go
 * quiet for that long: undici's default 300s headersTimeout used to abort the
 * daemon's fetch mid-wait (cancelling the interaction with reason
 * "request-aborted" at ~5 minutes), and proxies/tunnels carry idle timeouts
 * of their own. One space is valid JSON leading whitespace, so a slow drip
 * keeps those timers from firing without changing the payload the daemon
 * parses at completion.
 */
export const TOOL_CALL_HEARTBEAT_INTERVAL_MS = 30_000;

let heartbeatIntervalMs = TOOL_CALL_HEARTBEAT_INTERVAL_MS;

/** Test hook: shrink the drip so heartbeat behavior is observable in tests. */
export function setToolCallHeartbeatIntervalMs(ms: number): void {
  heartbeatIntervalMs = ms;
}

/**
 * Return the response head before a plugin tool finishes. Interactive plugin
 * tools can wait for user input for minutes, while bb Connect requires an
 * origin response head within 30 seconds. The response body can stay open.
 */
function streamToolCallResponse(result: Promise<ToolCallResponse>): Response {
  let heartbeat: ReturnType<typeof setInterval> | undefined;
  const stopHeartbeat = () => {
    if (heartbeat !== undefined) {
      clearInterval(heartbeat);
      heartbeat = undefined;
    }
  };
  const body = new ReadableStream<Uint8Array>({
    start(controller) {
      heartbeat = setInterval(() => {
        try {
          controller.enqueue(textEncoder.encode(" "));
        } catch {
          stopHeartbeat();
        }
      }, heartbeatIntervalMs);
      void result.then(
        (response) => {
          stopHeartbeat();
          try {
            controller.enqueue(textEncoder.encode(JSON.stringify(response)));
            controller.close();
          } catch (error) {
            controller.error(error);
          }
        },
        (error) => {
          stopHeartbeat();
          controller.error(error);
        },
      );
    },
    cancel() {
      stopHeartbeat();
    },
  });
  return new Response(body, {
    headers: {
      "content-type": "application/json; charset=UTF-8",
      // The app's global compress middleware would gzip this stream and hold
      // the one-byte heartbeats in the compressor's buffer, defeating the
      // drip (the daemon's client sends accept-encoding: gzip by default).
      // no-transform is the sanctioned opt-out — and the truth: middleboxes
      // must not buffer or re-encode a feed whose timing is the point.
      "cache-control": "no-transform",
    },
  });
}

export function registerInternalToolCallRoutes(app: Hono, deps: AppDeps): void {
  const { post } = typedRoutes<HostDaemonInternalSchema>(app, {
    onValidationError: (msg) => new ApiError(400, "invalid_request", msg),
  });

  post(
    "/session/tool-call",
    hostDaemonToolCallRequestSchema,
    async (context, payload) => {
      const session = requireAuthenticatedDaemonSession({
        context,
        db: deps.db,
        sessionId: payload.sessionId,
      });
      const { environment, thread } = requireThreadEnvironment(
        deps.db,
        payload.threadId,
      );
      if (environment.hostId !== session.hostId) {
        throw new ApiError(
          403,
          "invalid_request",
          "Thread does not belong to the session host",
        );
      }

      // Built-in tools win name lookups; then the native plugin-tool
      // registry (bb.agents.registerTool). A tool whose plugin was
      // disabled/reloaded away since the session started falls through to
      // the unsupported-tool response below.
      if (payload.tool === UPDATE_ENVIRONMENT_DIRECTORY_TOOL_NAME) {
        return context.json(
          await handleUpdateEnvironmentDirectoryToolCall(deps, {
            currentEnvironment: environment,
            input: payload.arguments,
            thread,
            turnId: payload.turnId,
          }),
        );
      }

      const pluginTool = findPluginAgentTool(payload.tool);
      if (pluginTool) {
        return streamToolCallResponse(
          invokePluginAgentTool(pluginTool, {
            input: payload.arguments,
            ctx: {
              threadId: thread.id,
              projectId: thread.projectId,
              // The request's own abort signal: it fires if the daemon
              // round-trip is torn down while the tool runs.
              signal: context.req.raw.signal,
            },
          }),
        );
      }

      return context.json({
        success: false,
        contentItems: [
          { type: "inputText", text: `Unsupported tool: ${payload.tool}` },
        ],
      });
    },
  );
}
