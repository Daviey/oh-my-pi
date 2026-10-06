import { afterAll, describe, expect, it } from "bun:test";
import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { HubClient } from "@oh-my-pi/pi-coding-agent/irc/remote/client";
import { startHubBroker } from "@oh-my-pi/pi-coding-agent/irc/remote/broker";
import type { HubElectionClientFrame, HubElectionServerFrame } from "@oh-my-pi/pi-coding-agent/irc/remote/protocol";
import type { HubClientLike } from "@oh-my-pi/pi-coding-agent/irc/remote/client";
import { claimWins, ElectionNode } from "@oh-my-pi/pi-coding-agent/irc/election";

function tmpSocketPath(): string {
	return path.join(fs.mkdtempSync(path.join(os.tmpdir(), "hub-election-")), "hub.sock");
}

const sockets: string[] = [];

afterAll(() => {
	for (const socketPath of sockets) {
		try {
			fs.rmSync(path.dirname(socketPath), { recursive: true, force: true });
		} catch {
			// already gone
		}
	}
});

/** Compact election timings. */
const TIGHT = { leaseTtlMs: 1_500, refreshMs: 400, heartbeatMs: 300, claimDelayMs: 20 };

function identity(agentId: string, project: string, sessionId: string) {
	return { agentId, project, status: "running" as const, pid: process.pid, sessionId };
}

/** Poll `check` (sync or async) until it passes or the deadline lapses. */
async function until(check: () => boolean | Promise<boolean>, deadlineMs: number): Promise<boolean> {
	const deadline = Date.now() + deadlineMs;
	for (;;) {
		if (await check()) return true;
		if (Date.now() >= deadline) return await check();
		await Bun.sleep(25);
	}
}

/**
 * Deterministic virtual clock: tasks fire only when `advance` moves time,
 * in schedule order — no real timers, no wall-clock waits.
 */
class ManualClock {
	#ms = 0;
	#seq = 0;
	#tasks = new Map<number, { at: number; run: () => void }>();

	readonly now = (): number => this.#ms;

	readonly schedule = (run: () => void, ms: number) => {
		const id = ++this.#seq;
		this.#tasks.set(id, { at: this.#ms + Math.max(0, ms), run });
		return {
			clear: () => {
				this.#tasks.delete(id);
			},
		};
	};

	advance(ms: number): void {
		this.#ms += ms;
		for (;;) {
			let due: { id: number; at: number } | undefined;
			for (const [id, task] of this.#tasks) {
				if (task.at <= this.#ms && (due === undefined || task.at < due.at || (task.at === due.at && id < due.id))) {
					due = { id, at: task.at };
				}
			}
			if (due === undefined) return;
			const task = this.#tasks.get(due.id);
			this.#tasks.delete(due.id);
			task?.run();
		}
	}
}

/** Frame wire between in-memory nodes: sender's broadcast reaches every
 *  other node synchronously (broker fan-out / frames-topic echo, minus
 *  the transport). */
class Wire {
	sent: HubElectionClientFrame[] = [];
	#sinks = new Map<ElectionNode, (frame: HubElectionServerFrame) => void>();

	hubFor(node: ElectionNode): HubClientLike {
		return {
			roster: async () => [],
			publish: async () => null,
			request: async () => null,
			onRequest: () => {},
			onElection: handler => {
				if (handler) this.#sinks.set(node, handler);
				else this.#sinks.delete(node);
			},
			sendElection: frame => {
				this.sent.push(frame);
				// Client and server election frame shapes are identical, so
				// the relayed copy is structurally the delivered frame.
				const delivered = frame as HubElectionServerFrame;
				for (const [peer, sink] of this.#sinks) {
					if (peer !== node) sink(delivered);
				}
			},
			setStatus: async () => {},
			onDelivery: () => {},
			onClose: () => {},
			close: () => {},
		};
	}
}

function node(sessionId: string, clock: ManualClock): ElectionNode {
	return new ElectionNode(sessionId, { ...TIGHT, now: clock.now, schedule: clock.schedule });
}

describe("election state machine (virtual clock)", () => {
	it("claimWins: earliest establishment wins, refresh always accepted, expired local loses", () => {
		const now = 10_000;
		const local = { leaderSessionId: "aaa", leaseUntil: now + 5_000, claimedAt: 1_000 };
		expect(claimWins({ leaderSessionId: "bbb", leaseUntil: now + 5_000, claimedAt: 2_000 }, local, now)).toBe(false);
		expect(claimWins({ leaderSessionId: "bbb", leaseUntil: now + 5_000, claimedAt: 500 }, local, now)).toBe(true);
		expect(claimWins({ leaderSessionId: "aaa", leaseUntil: now + 9_000, claimedAt: 2_000 }, local, now)).toBe(true);
		expect(claimWins({ leaderSessionId: "bbb", leaseUntil: now + 1, claimedAt: 9_999 }, { ...local, leaseUntil: now - 1 }, now)).toBe(true);
		expect(claimWins({ leaderSessionId: "aaa", leaseUntil: now + 5_000, claimedAt: 1_000 }, { ...local, leaderSessionId: "bbb" }, now)).toBe(true);
	});

	it("single node with no valid lease claims leader", () => {
		const clock = new ManualClock();
		const wire = new Wire();
		const solo = node("solo", clock);
		solo.attach(wire.hubFor(solo));
		expect(solo.electionRole()).toBe("member");
		clock.advance(TIGHT.claimDelayMs + 1);
		expect(solo.electionRole()).toBe("leader");
		const claims = wire.sent.filter(f => f.type === "leaderClaim");
		expect(claims.length).toBe(1);
		expect(claims[0]!.leaderSessionId).toBe("solo");
		expect(claims[0]!.leaseUntil).toBeGreaterThan(claims[0]!.claimedAt);
		solo.close();
	});

	it("two nodes contend: exactly one leader, the other settles as member", () => {
		const clock = new ManualClock();
		const wire = new Wire();
		const a = node("node-a", clock);
		const b = node("node-b", clock);
		a.attach(wire.hubFor(a));
		b.attach(wire.hubFor(b));

		clock.advance(TIGHT.claimDelayMs + 1);
		const roles = [a.electionRole(), b.electionRole()];
		expect(roles.filter(r => r === "leader").length).toBe(1);
		expect(roles.includes("member")).toBe(true);

		// Steady state across refresh cycles: the incumbent keeps winning.
		clock.advance(TIGHT.leaseTtlMs * 3);
		expect([a.electionRole(), b.electionRole()].filter(r => r === "leader").length).toBe(1);

		a.close();
		b.close();
	});

	it("leader assigns observed peers as middles; the assigned node reports middle", () => {
		const clock = new ManualClock();
		const wire = new Wire();
		const a = node("node-a", clock);
		const b = node("node-b", clock);
		a.attach(wire.hubFor(a));
		b.attach(wire.hubFor(b));
		clock.advance(TIGHT.claimDelayMs + 1);
		const leader = a.electionRole() === "leader" ? a : b;
		const follower = leader === a ? b : a;

		// The leader learns about the follower (roster observation) and its
		// next refresh carries the relay-slot assignment.
		leader.observePeers([follower.ownSessionId]);
		clock.advance(TIGHT.refreshMs + 1);
		expect(follower.electionRole()).toBe("middle");
		expect(follower.currentMiddles()).toContain(follower.ownSessionId);

		a.close();
		b.close();
	});

	it("leader death expires the lease and the survivor re-elects", () => {
		const clock = new ManualClock();
		const wire = new Wire();
		const a = node("node-a", clock);
		const b = node("node-b", clock);
		a.attach(wire.hubFor(a));
		b.attach(wire.hubFor(b));
		clock.advance(TIGHT.claimDelayMs + 1);
		const leader = a.electionRole() === "leader" ? a : b;
		const survivor = leader === a ? b : a;
		expect(survivor.electionRole()).not.toBe("leader");

		// Leader dies silently: no refresh, no goodbye frame.
		leader.close();
		// Past the expired lease (+1ms re-check slack) the survivor claims.
		clock.advance(TIGHT.leaseTtlMs + 2);
		expect(survivor.electionRole()).toBe("leader");
		expect(survivor.currentLease()?.leaderSessionId).toBe(survivor.ownSessionId);

		// Re-election reassigns slots: the survivor's claim is authoritative
		// now, and any stale middle assignment from the dead leader is gone.
		expect(survivor.roleFor(survivor.ownSessionId)).toBe("leader");
		survivor.close();
	});

	it("heartbeats are broadcast with a fresh lastSeen stamp", () => {
		const clock = new ManualClock();
		const wire = new Wire();
		const a = node("node-a", clock);
		a.attach(wire.hubFor(a));
		clock.advance(1);
		const beats = wire.sent.filter(f => f.type === "heartbeat");
		expect(beats.length).toBeGreaterThanOrEqual(1);
		expect(beats[0]!.lastSeen).toBe(clock.now());
		clock.advance(TIGHT.heartbeatMs + 1);
		const later = wire.sent.filter(f => f.type === "heartbeat");
		expect(later.at(-1)!.lastSeen).toBe(clock.now());
		a.close();
	});
});

describe("leader election (unix broker, real HubClients)", () => {
	// Real-clock integration tests: the in-process unix broker keeps its
	// own Node timers, so deterministic virtual clocks cannot drive it.
	// Conditions are POLLED (25ms), never fixed sleeps — the repo's
	// established pattern for broker tests (see hub.test.ts).
	function startBroker(socketPath: string): Promise<void> {
		const listening = Promise.withResolvers<void>();
		void startHubBroker({ socketPath, idleGraceMs: 60_000, onListening: listening.resolve });
		return listening.promise;
	}

	async function startPair(socketPath: string): Promise<{
		a: HubClient;
		b: HubClient;
		nodeA: ElectionNode;
		nodeB: ElectionNode;
	}> {
		await startBroker(socketPath);
		const a = await HubClient.connect({ socketPath, identity: identity("main", "proj", "node-a"), spawn: () => {} });
		const b = await HubClient.connect({ socketPath, identity: identity("main", "proj", "node-b"), spawn: () => {} });
		expect(a).not.toBeNull();
		expect(b).not.toBeNull();
		const nodeA = new ElectionNode("node-a", TIGHT);
		const nodeB = new ElectionNode("node-b", TIGHT);
		nodeA.attach(a!);
		nodeB.attach(b!);
		return { a: a!, b: b!, nodeA, nodeB };
	}

	it("two HubClients on one broker contend: exactly one leader", async () => {
		const socketPath = tmpSocketPath();
		sockets.push(socketPath);
		const { a, b, nodeA, nodeB } = await startPair(socketPath);

		// Exactly one leader AND the loser's lease view agrees with the
		// winner — the both-optimistic transient doesn't count as settled.
		const settled = await until(
			() => {
				const leaders = [nodeA.electionRole(), nodeB.electionRole()].filter(r => r === "leader");
				if (leaders.length !== 1) return false;
				const leader = nodeA.electionRole() === "leader" ? nodeA : nodeB;
				const loser = leader === nodeA ? nodeB : nodeA;
				return loser.currentLease()?.leaderSessionId === leader.ownSessionId;
			},
			5_000,
		);
		expect(settled).toBe(true);
		const roles = [nodeA.electionRole(), nodeB.electionRole()];
		expect(roles.filter(r => r === "leader").length).toBe(1);
		expect(roles.includes("member") || roles.includes("middle")).toBe(true);

		nodeA.close();
		nodeB.close();
		a.close();
		b.close();
	}, 15_000);

	it("leader lease expiry over the unix path re-elects the survivor", async () => {
		const socketPath = tmpSocketPath();
		sockets.push(socketPath);
		const { a, b, nodeA, nodeB } = await startPair(socketPath);

		expect(
			await until(() => [nodeA.electionRole(), nodeB.electionRole()].filter(r => r === "leader").length === 1, 5_000),
		).toBe(true);
		const leaderNodeIsA = nodeA.electionRole() === "leader";
		const leaderNode = leaderNodeIsA ? nodeA : nodeB;
		const survivor = leaderNodeIsA ? nodeB : nodeA;
		const leaderClient = leaderNodeIsA ? a : b;

		// Kill the leader hard: node timers stop, socket dies — no refresh.
		leaderNode.close();
		leaderClient.close();

		// Lease (1.5s) lapses; the survivor's expiry check re-claims.
		expect(await until(() => survivor.electionRole() === "leader", 8_000)).toBe(true);

		nodeA.close();
		nodeB.close();
		a.close();
		b.close();
	}, 20_000);

	it("heartbeats stamp roster lastSeen via the broker", async () => {
		const socketPath = tmpSocketPath();
		sockets.push(socketPath);
		const { a, b, nodeA, nodeB } = await startPair(socketPath);

		// Both nodes heartbeat (300ms); the broker stamps the sender's rows,
		// so b's roster view of a carries a fresh lastSeen.
		const stamped = await until(async () => {
			const roster = await b.roster();
			return roster.some(entry => typeof entry.lastSeen === "number" && entry.lastSeen > 0);
		}, 5_000);
		expect(stamped).toBe(true);
		// lastSeen ADVANCES across beats: poll until a strictly newer stamp
		// shows up (heartbeat interval is 300ms).
		const first = (await b.roster()).find(entry => typeof entry.lastSeen === "number")?.lastSeen ?? 0;
		const advanced = await until(async () => {
			const latest = (await b.roster()).find(entry => typeof entry.lastSeen === "number")?.lastSeen ?? 0;
			return latest > first;
		}, 5_000);
		expect(advanced).toBe(true);
		for (const entry of await b.roster()) {
			expect(typeof entry.lastSeen).toBe("number");
		}

		nodeA.close();
		nodeB.close();
		a.close();
		b.close();
	}, 15_000);

	it("leader abdicate over the real broker: follower takes over after claimDelay, not leaseTtl", async () => {
		const socketPath = tmpSocketPath();
		sockets.push(socketPath);
		const { a, b, nodeA, nodeB } = await startPair(socketPath);

		const settled = await until(
			() => [nodeA.electionRole(), nodeB.electionRole()].filter(r => r === "leader").length === 1,
			5_000,
		);
		expect(settled).toBe(true);
		const leaderNode = nodeA.electionRole() === "leader" ? nodeA : nodeB;
		const followerNode = leaderNode === nodeA ? nodeB : nodeA;

		const t0 = Date.now();
		leaderNode.close(); // broadcasts leaderAbdicate through the real client + broker
		const tookOver = await until(() => followerNode.electionRole() === "leader", 5_000);
		const elapsed = Date.now() - t0;
		expect(tookOver).toBe(true);
		// claimDelayMs is 20ms; waiting out the lease would take 1_500ms+.
		expect(elapsed).toBeLessThan(TIGHT.leaseTtlMs);

		followerNode.close();
		a.close();
		b.close();
	}, 15_000);
});

describe("graceful abdication", () => {
	it("leader's close() broadcasts abdicate; follower re-claims after claimDelay, not leaseTtl", () => {
		const clock = new ManualClock();
		const wire = new Wire();
		const a = node("node-a", clock);
		const b = node("node-b", clock);
		a.attach(wire.hubFor(a));
		b.attach(wire.hubFor(b));
		clock.advance(TIGHT.claimDelayMs + 1);
		expect([a.electionRole(), b.electionRole()].filter(r => r === "leader").length).toBe(1);
		const leader = a.electionRole() === "leader" ? a : b;
		const follower = leader === a ? b : a;

		leader.close();
		// Abdication reached the follower synchronously through the wire.
		expect(wire.sent.some(f => f.type === "leaderAbdicate")).toBe(true);
		// Far short of leaseTtl: the seat is re-contested after claimDelay.
		clock.advance(TIGHT.claimDelayMs + 1);
		expect(follower.electionRole()).toBe("leader");

		follower.close();
	});

	it("telemetry: claims counted, stale claims rejected, abdications seen", () => {
		const clock = new ManualClock();
		const stubClient = { handler: undefined as ((f: HubElectionServerFrame) => void) | undefined };
		const stub = {
			roster: async () => [],
			publish: async () => null,
			request: async () => null,
			onRequest: () => {},
			onElection: (h: ((f: HubElectionServerFrame) => void) | null) => { stubClient.handler = h ?? undefined; },
			sendElection: () => {},
			setStatus: async () => {},
			onDelivery: () => {},
			onClose: () => {},
			close: () => {},
		} as const;
		const c = node("node-c", clock);
		c.attach(stub as unknown as HubClientLike);
		const deliver = (frame: HubElectionServerFrame) => stubClient.handler!(frame);

		deliver({ type: "leaderClaim", leaderSessionId: "real-leader", leaseUntil: clock.now() + 5_000, claimedAt: clock.now() });
		expect(c.electionStats().claimsSeen).toBe(1);
		expect(c.electionStats().claimsRejected).toBe(0);
		expect(c.electionRole()).toBe("member");

		// Late establishment attempt: contender claims AFTER the incumbent
		// established — loses by earliest-establishment, rejected.
		deliver({ type: "leaderClaim", leaderSessionId: "contender", leaseUntil: clock.now() + 5_000, claimedAt: clock.now() + 1_000 });
		expect(c.electionStats().claimsSeen).toBe(2);
		expect(c.electionStats().claimsRejected).toBe(1);

		// Holder abdicates: counted, seat opens.
		deliver({ type: "leaderAbdicate", leaderSessionId: "real-leader" });
		expect(c.electionStats().abdicationsSeen).toBe(1);
		clock.advance(TIGHT.claimDelayMs + 1);
		expect(c.electionRole()).toBe("leader"); // c claimed the open seat
		expect(c.electionStats().roleFlips).toBeGreaterThanOrEqual(1);
		c.close();
	});

	it("non-holder close() emits no abdicate and does not disturb the leader", () => {
		const clock = new ManualClock();
		const wire = new Wire();
		const a = node("node-a", clock);
		const b = node("node-b", clock);
		a.attach(wire.hubFor(a));
		b.attach(wire.hubFor(b));
		clock.advance(TIGHT.claimDelayMs + 1);
		const leader = a.electionRole() === "leader" ? a : b;
		const follower = leader === a ? b : a;

		wire.sent.length = 0;
		follower.close(); // never held the lease
		expect(wire.sent.some(f => f.type === "leaderAbdicate")).toBe(false);
		expect(leader.electionRole()).toBe("leader");
		leader.close();
	});
});
