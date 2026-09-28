import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import * as path from "node:path";
import { Agent } from "@oh-my-pi/pi-agent-core";
import { createMockModel } from "@oh-my-pi/pi-ai/providers/mock";
import { getBundledModel } from "@oh-my-pi/pi-catalog/models";
import { ModelRegistry } from "@oh-my-pi/pi-coding-agent/config/model-registry";
import { Settings } from "@oh-my-pi/pi-coding-agent/config/settings";
import { AgentSession } from "@oh-my-pi/pi-coding-agent/session/agent-session";
import type { AuthStorage } from "@oh-my-pi/pi-coding-agent/session/auth-storage";
import { convertToLlm } from "@oh-my-pi/pi-coding-agent/session/messages";
import { SessionManager } from "@oh-my-pi/pi-coding-agent/session/session-manager";
import { IrcBus } from "@oh-my-pi/pi-coding-agent/irc/bus";
import { AgentRegistry } from "@oh-my-pi/pi-coding-agent/registry/agent-registry";
import type { HubClientLike, HubRequestHandler } from "@oh-my-pi/pi-coding-agent/irc/remote/client";
import { TempDir } from "@oh-my-pi/pi-utils";
import type { IrcMessage } from "@oh-my-pi/pi-tui/tools/irc";
import { createInMemoryAuthStorage } from "../helpers/agent-session-setup";

/**
 * Regression tests for the hub RPC handler lifecycle in long-lived
 * processes. Two live-observed decays:
 *
 * 1. Every AgentSession constructor bound IrcBus.global()'s hub request
 *    handler — including short-lived SUBAGENT sessions — so a subagent
 *    spawn+dispose left the process-global handler pointing at a disposed
 *    session whose deliverIrcMessage throws; the transports swallowed the
 *    throw and the process silently stopped answering hub requests.
 *
 * 2. The main-agent wake relay skipped any wake source whose `from` equals
 *    selfId. Cross-process hub RPC between two processes whose main agents
 *    are both "Main" hit exactly that skip: the relay never armed, the
 *    correlated reply never fired, and every reply degraded to the 120s
 *    [rpc-timeout] fallback.
 */

function buildSession(agentKind: "main" | "sub"): {
	session: AgentSession;
	dispose: () => Promise<void>;
} {
	const tempDir = TempDir.createSync("@pi-hub-rpc-lifecycle-");
	const model = getBundledModel("anthropic", "claude-sonnet-4-5");
	if (!model) throw new Error("Expected claude-sonnet-4-5 to exist");
	const authStorage: AuthStorage = createInMemoryAuthStorage();
	authStorage.keys.setRuntime("anthropic", "test-key");
	const modelRegistry = new ModelRegistry(
		authStorage,
		path.join(tempDir.path(), "models.yml"),
	);
	const settings = Settings.isolated({ "compaction.enabled": false });
	const sessionManager = SessionManager.inMemory(tempDir.path());
	const mock = createMockModel({
		handler: () => ({
			content: [{ type: "text" as const, text: "turn output" }],
		}),
	});
	const agent = new Agent({
		getApiKey: () => "test-key",
		initialState: { model, systemPrompt: ["Test"], tools: [], messages: [] },
		convertToLlm,
		streamFn: mock.stream,
	});
	const session = new AgentSession({
		agent,
		sessionManager,
		settings,
		modelRegistry,
		toolRegistry: new Map(),
		agentKind,
	});
	return {
		session,
		dispose: async () => {
			await session.dispose();
			authStorage.close();
			tempDir.removeSync();
		},
	};
}

const requestMessage = (requestId: string, from: string): IrcMessage => ({
	id: requestId,
	from,
	to: "Main",
	body: "remote question",
	ts: Date.now(),
});

/** Minimal hub-client double: captures the transport sinks hub-manager wires. */
function attachFakeHubClient(bus: IrcBus): {
	requestSink: (
		msg: IrcMessage,
		from: string,
	) => Promise<IrcMessage | null> | IrcMessage | null;
	deliverySink: (msg: IrcMessage) => void;
} {
	let requestSink!: (
		msg: IrcMessage,
		from: string,
	) => Promise<IrcMessage | null> | IrcMessage | null;
	let deliverySink!: (msg: IrcMessage) => void;
	const fakeClient: HubClientLike = {
		onDelivery: (sink: (msg: IrcMessage) => void) => {
			deliverySink = sink;
		},
		onRequest: (sink: HubRequestHandler) => {
			requestSink = sink;
		},
		// Unused by the lifecycle paths under test:
		roster: () => Promise.reject(new Error("not implemented")),
		request: () => Promise.resolve(null),
		setStatus: () => Promise.resolve(),
		onClose: () => {},
		publish: async () => ({ results: [] }),
		close: () => {},
	};
	bus.attachHubClient(fakeClient);
	return {
		requestSink: (msg, from) => requestSink(msg, from),
		deliverySink: (msg) => deliverySink(msg),
	};
}

describe("hub RPC handler lifecycle", () => {
	beforeEach(() => {
		IrcBus.resetGlobalForTests();
		AgentRegistry.resetGlobalForTests();
	});

	const disposers: (() => Promise<void>)[] = [];
	afterEach(async () => {
		for (const dispose of disposers.splice(0).reverse())
			await dispose().catch(() => {});
	});

	it("subagent construction does not steal the main session's hub request handler", async () => {
		// A long-lived main session binds the process-global handler...
		const main = buildSession("main");
		disposers.push(main.dispose);
		const bus = IrcBus.global();
		const hub = attachFakeHubClient(bus);

		// ...then subagent churn: a short-lived sub session constructs (its
		// ctor runs the same constructor body) and is disposed — exactly the
		// live decay sequence. Pre-fix, the sub ctor re-bound the bus-global
		// handler to the sub's own closure; its dispose stranded that closure
		// there, and every later request threw inside the disposed session.
		const sub = buildSession("sub");
		await sub.dispose();

		// A peer request must still reach the LIVE main session and come back
		// with the main turn's output (mock model answers "turn output").
		const reply = (await hub.requestSink(
			requestMessage("req-steal-1", "Peer"),
			"Peer",
		)) as IrcMessage | null;
		expect(reply).not.toBeNull();
		expect(reply!.replyTo).toBe("req-steal-1");
		expect(reply!.body).toContain("turn output");
	}, 20_000);

	it("cross-process Main→Main request arms the wake relay and replies with the turn output", async () => {
		const main = buildSession("main");
		disposers.push(main.dispose);
		const bus = IrcBus.global();
		const hub = attachFakeHubClient(bus);

		// Requester and answerer share the id "Main" (two processes, one id
		// each) — the `from === selfId` wake-source skip used to drop this
		// source, so the relay never armed and the reply never resolved.
		const reply = (await hub.requestSink(
			requestMessage("req-same-id-1", "Main"),
			"Main",
		)) as IrcMessage | null;
		expect(reply).not.toBeNull();
		expect(reply!.replyTo).toBe("req-same-id-1");
		// The agent's actual turn output — NOT the 120s fallback body the
		// un-armed relay path produced.
		expect(reply!.body).toBe("turn output");
		expect(reply!.body).not.toContain("rpc-timeout");
	}, 20_000);
});
