import { afterEach, describe, expect, it } from "bun:test";
import type { Agent, AgentMessage } from "@oh-my-pi/pi-agent-core";
import { AgentRegistry } from "@oh-my-pi/pi-coding-agent/registry/agent-registry";
import { IrcBridge, type IrcBridgeHost } from "@oh-my-pi/pi-coding-agent/session/irc-bridge";
import { stampSenderSession } from "@oh-my-pi/pi-coding-agent/irc/remote/protocol";
import type { IrcMessage } from "@oh-my-pi/pi-tui/tools/irc";

function makeBridge() {
	const woken: AgentMessage[][] = [];
	const host = {
		agent: { steer: () => {} } as unknown as Agent,
		isDisposed: () => false,
		isStreaming: () => false,
		planModeEnabled: () => false,
		emitSessionEvent: async () => {},
		wakeForIrc: (records: AgentMessage[]) => woken.push(records),
	} as unknown as IrcBridgeHost;
	return { bridge: new IrcBridge(host), woken };
}

function recordText(records: AgentMessage[]): string {
	const first = records[0] as { content?: unknown } | undefined;
	const content = first?.content;
	return typeof content === "string" ? content : JSON.stringify(content ?? "");
}

afterEach(() => {
	AgentRegistry.resetGlobalForTests();
});

describe("IRC reply addressing", () => {
	it("stamps the sender's session id onto outbound peer messages", () => {
		const msg: IrcMessage = { id: "m1", from: "Main", to: "Peer", body: "hi", ts: 1 };
		const stamped = stampSenderSession(msg, { sessionId: "sess-abc" });
		expect(stamped.fromSessionId).toBe("sess-abc");
		// The caller's message object is never mutated.
		expect(msg.fromSessionId).toBeUndefined();
		// An explicit stamp wins (forwarding another peer's message).
		expect(stampSenderSession({ ...msg, fromSessionId: "original" }, { sessionId: "mine" }).fromSessionId).toBe("original");
		// No session id on the identity: message passes through unchanged.
		expect(stampSenderSession(msg, undefined)).toBe(msg);
	});

	it("addresses replies by session id when the sender stamped one", async () => {
		const { bridge, woken } = makeBridge();
		await bridge.deliver({ id: "m2", from: "Main", to: "Main", body: "cross-project ping", ts: 1, fromSessionId: "01a10315-peer" });
		const text = recordText(woken[0]);
		expect(text).toContain("[sender session: 01a10315-peer]");
		expect(text).toContain('path: "agent://session:01a10315-peer"');
		expect(text).toContain("to `session:01a10315-peer`");
		// The ambiguous bare-id advice must not appear once a session id is known.
		expect(text).not.toContain('path: "agent://Main"');
	});

	it("falls back to the bare agent id with a caveat for pre-session senders", async () => {
		const { bridge, woken } = makeBridge();
		await bridge.deliver({ id: "m3", from: "OldPeer", to: "Main", body: "legacy hello", ts: 1 });
		const text = recordText(woken[0]);
		expect(text).toContain('path: "agent://OldPeer"');
		expect(text).toContain("to `OldPeer`");
		expect(text).toContain("session id not stamped");
		expect(text).not.toContain("[sender session:");
	});

	it("keeps from and fromSessionId in the persisted details", async () => {
		const { bridge, woken } = makeBridge();
		await bridge.deliver({ id: "m4", from: "Main", to: "Main", body: "x", ts: 1, fromSessionId: "sess-d" });
		const details = (woken[0][0] as { details?: Record<string, unknown> }).details ?? {};
		expect(details.from).toBe("Main");
		expect(details.fromSessionId).toBe("sess-d");
	});
});
