import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import { IrcBus } from "@oh-my-pi/pi-coding-agent/irc/bus";
import { AgentRegistry } from "@oh-my-pi/pi-coding-agent/registry/agent-registry";
import { executeRequest } from "@oh-my-pi/pi-coding-agent/irc/messaging";
import type { IrcMessage } from "@oh-my-pi/pi-tui/tools/irc";
import { AgentSession } from "@oh-my-pi/pi-coding-agent/session/agent-session";
import { TempDir } from "@oh-my-pi/pi-utils";

/**
 * Regression tests for the correlated-request reply path: a request's wait
 * resolves ONLY on `candidate.replyTo === requestId` (see executeRequest).
 * Three behaviors guarded:
 *
 * 1. The wire id of the outbound request IS the correlation id: bus.send
 *    preserves a caller-supplied id (used to be overwritten with a fresh
 *    Snowflake, which made local requests structurally unable to correlate).
 *
 * 2. The requester must observe SUCCESSFUL deliveries: a same-process reply
 *    consumed by the recipient's session (steer/wake injection) never
 *    buffers to the mailbox, so executeRequest's takeMatching poll alone
 *    only ever saw FAILED deliveries. IrcBus.onDeliver (fired on waiter
 *    resolution and post-injection) closes that gap.
 *
 * 3. The mailbox fallback: a reply whose delivery FAILED buffers via
 *    #enqueue and still correlates via the takeMatching scan.
 *
 * The peer stubs below reply from INSIDE deliverIrcMessage, echoing
 * `msg.id` as replyTo — no timers, no guessed awaits: the reply rides the
 * request's own delivery event.
 */

function sessionFor(deliver: (msg: IrcMessage) => Promise<"injected" | "woken">): AgentSession {
	// Lightweight stand-in: executeRequest's deps only reach the registry,
	// whose refs carry the session — the session here only needs
	// deliverIrcMessage for the deliver path under test.
	return { deliverIrcMessage: deliver } as unknown as AgentSession;
}

describe("correlated request replies", () => {
	beforeEach(() => {
		IrcBus.resetGlobalForTests();
		AgentRegistry.resetGlobalForTests();
	});

	const disposers: (() => Promise<void>)[] = [];
	afterEach(async () => {
		await Promise.all(disposers.splice(0).map(dispose => dispose()));
	});

	it("resolves when the peer's reply echoes the request id as replyTo (onDeliver path)", async () => {
		const registry = AgentRegistry.global();
		const bus = IrcBus.global();
		// Requester: a live session whose delivery consumes the reply (the
		// steer/wake path) — without onDeliver this times out, because a
		// successful delivery never buffers to the mailbox.
		registry.register({
			id: "Main",
			displayName: "Main",
			kind: "main",
			status: "running",
			session: sessionFor(async () => "injected"),
		});
		// Peer: sees the request via deliverIrcMessage and replies with
		// replyTo = the request's wire id (the correlation contract).
		registry.register({
			id: "Peer",
			displayName: "Peer",
			kind: "main",
			status: "running",
			session: sessionFor(async msg => {
				await bus.send({ from: "Peer", to: "Main", body: "ACK control", replyTo: msg.id });
				return "injected";
			}),
		});
		const tempDir = TempDir.createSync("@pi-correlated-replies-");
		disposers.push(async () => tempDir.removeSync());

		const result = await executeRequest(
			{ registry, senderId: "Main", sessionFileHint: null },
			{ to: "Peer", message: "are you there?", timeoutMs: 5_000 },
		);
		const text = result.content.find(item => item.type === "text");
		// Success returns carry no isError flag — its presence means the
		// timeout error path fired.
		expect(result.isError).toBeUndefined();
		expect(text?.text).toContain("ACK control");
	});

	it("falls back to the mailbox when delivery to the requester FAILED", async () => {
		const registry = AgentRegistry.global();
		const bus = IrcBus.global();
		registry.register({
			id: "Peer",
			displayName: "Peer",
			kind: "main",
			status: "running",
			session: sessionFor(async msg => {
				await bus.send({ from: "Peer", to: "Main", body: "ACK buffered", replyTo: msg.id });
				return "injected";
			}),
		});
		// Requester's delivery THROWS → #deliver's catch runs #enqueue,
		// buffering the reply into the mailbox for the takeMatching scan.
		registry.register({
			id: "Main",
			displayName: "Main",
			kind: "main",
			status: "running",
			session: sessionFor(async () => {
				throw new Error("delivery fails");
			}),
		});
		const tempDir = TempDir.createSync("@pi-correlated-replies-2-");
		disposers.push(async () => tempDir.removeSync());

		const result = await executeRequest(
			{ registry, senderId: "Main", sessionFileHint: null },
			{ to: "Peer", message: "are you there?", timeoutMs: 5_000 },
		);
		const text = result.content.find(item => item.type === "text");
		expect(result.isError).toBeUndefined();
		expect(text?.text).toContain("ACK buffered");
	});

	it("times out when an uncorrelated reply arrives (no replyTo)", async () => {
		const registry = AgentRegistry.global();
		const bus = IrcBus.global();
		registry.register({
			id: "Main",
			displayName: "Main",
			kind: "main",
			status: "running",
			session: sessionFor(async () => "injected"),
		});
		registry.register({
			id: "Peer",
			displayName: "Peer",
			kind: "main",
			status: "running",
			session: sessionFor(async msg => {
				// Replies WITHOUT replyTo must not correlate — this is the
				// exact silent-drop failure the ?replyTo= surface fixes.
				await bus.send({ from: "Peer", to: "Main", body: "uncorrelated ACK" });
				return "injected";
			}),
		});
		const tempDir = TempDir.createSync("@pi-correlated-replies-3-");
		disposers.push(async () => tempDir.removeSync());

		const result = await executeRequest(
			{ registry, senderId: "Main", sessionFileHint: null },
			{ to: "Peer", message: "are you there?", timeoutMs: 1_500 },
		);
		expect(result.isError).toBe(true);
		const text = result.content.find(item => item.type === "text");
		expect(text?.text).toContain("timed out");
	});
});
