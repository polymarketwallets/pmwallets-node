import { createInterface } from 'node:readline';
import type { Readable, Writable } from 'node:stream';

/**
 * A Model Context Protocol server over stdio (newline-delimited JSON-RPC 2.0), tools only. Small enough to carry
 * no dependency: initialize, ping, tools/list and tools/call are all an agent host needs from it.
 * Anything written to stdout that is not a protocol message breaks the host — log to stderr only.
 */
export interface McpTool {
  name: string;
  description: string;
  /** JSON Schema of the arguments object */
  inputSchema: Record<string, unknown>;
  /** the result is sent to the agent as JSON text; a throw becomes a tool error the agent can read */
  handler: (args: Record<string, unknown>) => Promise<unknown>;
}

export interface McpServerOptions {
  name: string;
  version: string;
  tools: McpTool[];
  /** shown to the agent at connect: what the server is for and the rules of using it */
  instructions?: string;
  input?: Readable;
  output?: Writable;
}

export const MCP_PROTOCOL_VERSIONS = ['2025-06-18', '2025-03-26', '2024-11-05'];

interface Request { jsonrpc?: string; id?: string | number | null; method?: unknown; params?: any }

/** Serves until the input ends. */
export async function serveMcp(opts: McpServerOptions): Promise<void> {
  const input = opts.input ?? process.stdin;
  const output = opts.output ?? process.stdout;
  const tools = new Map(opts.tools.map((t) => [t.name, t]));
  const send = (msg: unknown) => { output.write(`${JSON.stringify(msg)}\n`); };
  const reply = (id: Request['id'], result: unknown) => send({ jsonrpc: '2.0', id, result });
  const fail = (id: Request['id'], code: number, message: string) => send({ jsonrpc: '2.0', id: id ?? null, error: { code, message } });

  const handle = async (req: Request): Promise<void> => {
    const isCall = req.id !== undefined && req.id !== null;
    if (typeof req.method !== 'string') { if (isCall) fail(req.id, -32600, 'invalid request'); return; }
    switch (req.method) {
      case 'initialize': {
        const asked = req.params?.protocolVersion;
        reply(req.id, {
          protocolVersion: MCP_PROTOCOL_VERSIONS.includes(asked) ? asked : MCP_PROTOCOL_VERSIONS[0],
          capabilities: { tools: { listChanged: false } },
          serverInfo: { name: opts.name, version: opts.version },
          ...(opts.instructions ? { instructions: opts.instructions } : {}),
        });
        return;
      }
      case 'ping': reply(req.id, {}); return;
      case 'tools/list':
        reply(req.id, { tools: opts.tools.map(({ name, description, inputSchema }) => ({ name, description, inputSchema })) });
        return;
      case 'tools/call': {
        const tool = tools.get(String(req.params?.name ?? ''));
        if (!tool) { fail(req.id, -32602, `unknown tool ${JSON.stringify(req.params?.name ?? null)}`); return; }
        const args = req.params?.arguments;
        if (args !== undefined && (typeof args !== 'object' || args === null || Array.isArray(args))) { fail(req.id, -32602, 'arguments must be an object'); return; }
        try {
          const result = await tool.handler((args ?? {}) as Record<string, unknown>);
          reply(req.id, { content: [{ type: 'text', text: typeof result === 'string' ? result : JSON.stringify(result, null, 2) }] });
        } catch (e) {
          reply(req.id, { content: [{ type: 'text', text: errorText(e) }], isError: true });
        }
        return;
      }
      default:
        // notifications (initialized, cancelled, …) need no answer; an unknown request does
        if (isCall) fail(req.id, -32601, `method not found: ${req.method}`);
    }
  };

  const rl = createInterface({ input, crlfDelay: Infinity });
  const inflight = new Set<Promise<void>>();
  for await (const line of rl) {
    if (!line.trim()) continue;
    let req: Request;
    try { req = JSON.parse(line) as Request; } catch { fail(null, -32700, 'parse error'); continue; }
    if (!req || typeof req !== 'object' || Array.isArray(req)) { fail(null, -32600, 'invalid request'); continue; }
    const p = handle(req).catch((e) => fail(req.id, -32603, errorText(e)));
    inflight.add(p);
    void p.finally(() => inflight.delete(p));
  }
  await Promise.all(inflight);
}

function errorText(e: unknown): string {
  if (e && typeof e === 'object' && 'status' in e && 'body' in e) {
    const { status, body } = e as { status: number; body: unknown };
    return `HTTP ${status}: ${typeof body === 'string' ? body : JSON.stringify(body)}`;
  }
  return e instanceof Error ? e.message : String(e);
}
