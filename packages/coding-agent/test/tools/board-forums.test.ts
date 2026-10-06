import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import {
	boardStateBody,
	executeBoardOp,
	executeForumRead,
	executeSend,
	ingestForumFrame,
	ingestHubRoster,
	listForums,
	renderBoard,
	resetForumForTests,
	setBoardClockForTests,
} from "../../src/irc/messaging";
import { ElectionNode } from "../../src/irc/election";
import { electionSnapshot, installElectionForTests } from "../../src/irc/remote/hub-manager";

const frame = (over: Partial<Parameters<typeof ingestForumFrame>[0]> & { channel: string }) =>
	ingestForumFrame({ kind: "forum", from: "a", body: "", ts: 1, ...over } as never);

describe("work board (forum-channel backed)", () => {
	beforeEach(() => {
		resetForumForTests();
		setBoardClockForTests(() => 5_000); // near the synthetic frame ts values below
	});

	it("derives item state from the ingest stream: post → claim → done", () => {
		frame({ channel: "board", body: JSON.stringify({ board: true, op: "post", id: "w1", title: "fix routing" }), ts: 1 });
		frame({ channel: "board", body: JSON.stringify({ board: true, op: "claim", id: "w1", owner: "Main" }), ts: 2 });
		frame({ channel: "board", body: JSON.stringify({ board: true, op: "post", id: "w2", title: "ship docs" }), ts: 3 });
		const out = renderBoard();
		expect(out).toContain("CLAIMED");
		expect(out).toContain("w1");
		expect(out).toContain("fix routing");
		expect(out).toContain("(owner: Main)");
		expect(out).toContain("OPEN");
		expect(out).toContain("ship docs");
		frame({ channel: "board", body: JSON.stringify({ board: true, op: "done", id: "w1" }), ts: 4 });
		expect(renderBoard()).toContain("DONE");
	});

	it("ignores plain-text chatter on the board channel", () => {
		frame({ channel: "board", body: "just talking", ts: 1 });
		expect(renderBoard()).toContain("board empty");
	});

	it("claim/done against an unknown id is a no-op", () => {
		frame({ channel: "board", body: JSON.stringify({ board: true, op: "claim", id: "ghost" }), ts: 1 });
		expect(renderBoard()).toContain("board empty");
	});

	it("post without title errors; claim without id errors; no hub client errors — never throws", async () => {
		const deps = { registry: undefined as never, senderId: "Main" };
		expect((await executeBoardOp(deps, { op: "post" })).isError).toBe(true);
		expect((await executeBoardOp(deps, { op: "claim" })).isError).toBe(true);
		expect((await executeBoardOp(deps, { op: "post", title: "x" })).isError).toBe(true); // no client
	});
});

describe("board liveness + state snapshots", () => {
	beforeEach(() => {
		resetForumForTests();
		setBoardClockForTests(() => 15_000); // above the newest synthetic frame ts used here
	});

	it("claims key on fromSessionId: two Mains are distinct owners", () => {
		frame({ channel: "board", from: "Main", fromSessionId: "sess-1", body: JSON.stringify({ board: true, op: "post", id: "k1", title: "t" }), ts: 1 });
		frame({ channel: "board", from: "Main", fromSessionId: "sess-2", body: JSON.stringify({ board: true, op: "claim", id: "k1", owner: "Main" }), ts: 2 });
		ingestHubRoster([{ sessionId: "sess-1" }]); // sess-2 gone
		const board = renderBoard();
		expect(board).toContain("OPEN"); // sess-2's claim reaped
	});

	it("claimed item whose owner session is live stays claimed", () => {
		frame({ channel: "board", from: "Main", fromSessionId: "sess-1", body: JSON.stringify({ board: true, op: "post", id: "k2", title: "t" }), ts: 1 });
		frame({ channel: "board", from: "Main", fromSessionId: "sess-1", body: JSON.stringify({ board: true, op: "claim", id: "k2", owner: "Main" }), ts: 2 });
		ingestHubRoster([{ sessionId: "sess-1" }]);
		expect(renderBoard()).toContain("CLAIMED");
	});

	it("done items are never reaped", () => {
		frame({ channel: "board", from: "Main", fromSessionId: "sess-1", body: JSON.stringify({ board: true, op: "post", id: "k3", title: "t" }), ts: 1 });
		frame({ channel: "board", from: "Main", fromSessionId: "sess-1", body: JSON.stringify({ board: true, op: "done", id: "k3", owner: "Main" }), ts: 2 });
		ingestHubRoster([]); // owner gone
		expect(renderBoard()).toContain("DONE");
		expect(renderBoard()).not.toContain("OPEN");
	});

	it("state snapshot replaces all items, older replays ignored", () => {
		frame({ channel: "board", from: "leader", body: JSON.stringify({ board: true, op: "state", items: [{ id: "s1", title: "from snapshot", owner: "X", status: "claimed", postedAt: 5 }] }), ts: 10 });
		expect(renderBoard()).toContain("CLAIMED");
		frame({ channel: "board", from: "leader", body: JSON.stringify({ board: true, op: "state", items: [{ id: "s2", title: "older", status: "open", postedAt: 1 }] }), ts: 9 }); // stale replay
		expect(renderBoard()).toContain("s1");
		frame({ channel: "board", from: "leader", body: JSON.stringify({ board: true, op: "state", items: [{ id: "s2", title: "newer", status: "open", postedAt: 20 }] }), ts: 11 });
		const board = renderBoard();
		expect(board).toContain("s2");
		expect(board).not.toContain("s1");
	});

	it("boardStateBody round-trips into a state fold", () => {
		frame({ channel: "board", from: "Main", fromSessionId: "sess-9", body: JSON.stringify({ board: true, op: "post", id: "r1", title: "round trip" }), ts: 1 });
		frame({ channel: "board", from: "Main", fromSessionId: "sess-9", body: JSON.stringify({ board: true, op: "claim", id: "r1", owner: "Main" }), ts: 2 });
		const body = boardStateBody();
		resetForumForTests();
		frame({ channel: "board", from: "leader", body, ts: 3 });
		const board = renderBoard();
		expect(board).toContain("r1");
		expect(board).toContain("CLAIMED");
	});
});

describe("board claim leases (release + TTL)", () => {
	beforeEach(() => resetForumForTests());
	afterEach(() => setBoardClockForTests());

	it("release explicitly reverts a claim; done/open are unaffected", () => {
		setBoardClockForTests(() => 1_000_005);
		frame({ channel: "board", from: "Main", fromSessionId: "s1", body: JSON.stringify({ board: true, op: "post", id: "x1", title: "t" }), ts: 1_000_000 });
		frame({ channel: "board", from: "Main", fromSessionId: "s1", body: JSON.stringify({ board: true, op: "claim", id: "x1", owner: "Main" }), ts: 1_000_001 });
		frame({ channel: "board", from: "Main", fromSessionId: "s1", body: JSON.stringify({ board: true, op: "release", id: "x1", owner: "Main" }), ts: 1_000_002 });
		expect(renderBoard()).toContain("OPEN");
		frame({ channel: "board", from: "Main", fromSessionId: "s1", body: JSON.stringify({ board: true, op: "done", id: "x1", owner: "Main" }), ts: 1_000_003 });
		frame({ channel: "board", from: "Main", fromSessionId: "s1", body: JSON.stringify({ board: true, op: "release", id: "x1", owner: "Main" }), ts: 1_000_004 });
		expect(renderBoard()).toContain("DONE");
		expect(renderBoard()).not.toContain("OPEN");
	});

	it("claims expire after the TTL even with a live owner", () => {
		setBoardClockForTests(() => 1_000_000);
		frame({ channel: "board", from: "Main", fromSessionId: "s1", body: JSON.stringify({ board: true, op: "post", id: "e1", title: "t" }), ts: 1_000_000 });
		frame({ channel: "board", from: "Main", fromSessionId: "s1", body: JSON.stringify({ board: true, op: "claim", id: "e1", owner: "Main" }), ts: 1_000_001 });
		ingestHubRoster([{ sessionId: "s1" }]); // owner live — liveness reap must NOT fire
		setBoardClockForTests(() => 1_000_001 + 30 * 60_000); // exactly at the claim's TTL boundary: not yet expired
		expect(renderBoard()).toContain("CLAIMED");
		setBoardClockForTests(() => 1_000_001 + 30 * 60_000 + 1); // one ms past the claim's TTL
		expect(renderBoard()).toContain("OPEN");
	});

	it("leader state snapshots never renew a lease (claimTs round-trips)", () => {
		setBoardClockForTests(() => 1_000_000);
		frame({ channel: "board", from: "Main", fromSessionId: "s1", body: JSON.stringify({ board: true, op: "post", id: "q1", title: "t" }), ts: 1_000_000 });
		frame({ channel: "board", from: "Main", fromSessionId: "s1", body: JSON.stringify({ board: true, op: "claim", id: "q1", owner: "Main" }), ts: 1_000_001 });
		ingestHubRoster([{ sessionId: "s1" }]);
		// leader snapshot every 60s, far past the claim: claimTs must ride along
		for (let m = 1; m <= 45; m++) {
			const ts = 1_000_001 + m * 60_000;
			setBoardClockForTests(() => ts);
			frame({ channel: "board", from: "leader", body: boardStateBody(), ts });
		}
		// 45 min of snapshots after the claim: lease expired 15 min ago
		expect(renderBoard()).toContain("OPEN");
		// and the expired claim is what the NEXT snapshot broadcasts
		expect(boardStateBody()).not.toContain("claimed");
	});

	it("re-claiming renews the lease", () => {
		setBoardClockForTests(() => 1_000_000);
		frame({ channel: "board", from: "Main", fromSessionId: "s1", body: JSON.stringify({ board: true, op: "post", id: "n1", title: "t" }), ts: 1_000_000 });
		frame({ channel: "board", from: "Main", fromSessionId: "s1", body: JSON.stringify({ board: true, op: "claim", id: "n1", owner: "Main" }), ts: 1_000_001 });
		setBoardClockForTests(() => 1_000_000 + 20 * 60_000); // 20 min in: renew
		frame({ channel: "board", from: "Main", fromSessionId: "s1", body: JSON.stringify({ board: true, op: "claim", id: "n1", owner: "Main" }), ts: 1_000_000 + 20 * 60_000 });
		setBoardClockForTests(() => 1_000_000 + 20 * 60_000 + 30 * 60_000); // TTL after renewal, not after first claim
		expect(renderBoard()).toContain("CLAIMED");
		setBoardClockForTests(() => 1_000_000 + 20 * 60_000 + 30 * 60_000 + 1);
		expect(renderBoard()).toContain("OPEN");
	});

	it("done items never TTL-expire", () => {
		setBoardClockForTests(() => 1_000_000);
		frame({ channel: "board", from: "Main", fromSessionId: "s1", body: JSON.stringify({ board: true, op: "post", id: "d1", title: "t" }), ts: 1_000_000 });
		frame({ channel: "board", from: "Main", fromSessionId: "s1", body: JSON.stringify({ board: true, op: "done", id: "d1", owner: "Main" }), ts: 1_000_001 });
		setBoardClockForTests(() => 1_000_000 + 10 * 24 * 60 * 60_000); // 10 days later
		const board = renderBoard();
		expect(board).toContain("DONE");
		expect(board).not.toContain("OPEN");
	});
});

describe("forum census", () => {
	beforeEach(() => resetForumForTests());

	it("lists channels newest-activity-first with participants", () => {
		frame({ channel: "triage", from: "a", body: "hi", ts: 5 });
		frame({ channel: "triage", from: "b", body: "yo", ts: 6 });
		frame({ channel: "design", from: "c", body: "spec", ts: 9 });
		const forums = listForums();
		expect(forums.map(f => f.channel)).toEqual(["design", "triage"]);
		expect(forums[1]!.participants).toEqual(["a", "b"]);
		expect(forums[1]!.messages).toBe(2);
	});
});

describe("agent://leader alias (send path)", () => {
	it("errors cleanly when no leader is elected", async () => {
		const result = await executeSend(
			{ registry: undefined as never, senderId: "Main" },
			{ to: "leader", message: "who owns the deploy?" },
		);
		expect(result.isError).toBe(true);
		expect(result.content[0]).toMatchObject({ type: "text" });
	});

	it("refuses to self-deliver when this session IS the leader", async () => {
		const node = new ElectionNode("sess-self", { leaseTtlMs: 60_000, refreshMs: 20_000, heartbeatMs: 30_000, claimDelayMs: 10 });
		node.attach({
			roster: async () => [],
			publish: async () => null,
			request: async () => null,
			onElection: () => {},
			sendElection: () => {},
			onRequest: () => {},
			setStatus: async () => {},
			onDelivery: () => {},
			onClose: () => {},
			close: () => {},
		} satisfies import("../../src/irc/remote/client").HubClientLike);
		installElectionForTests(node);
		await Bun.sleep(60); // > claimDelayMs: solo node with no lease claims leadership
		expect(electionSnapshot().role).toBe("leader");
		const result = await executeSend(
			{ registry: undefined as never, senderId: "Main" },
			{ to: "leader", message: "escalate this" },
		);
		expect(result.isError).toBe(true);
		const text = result.content[0] as { type: string; text?: string };
		expect(text.text).toContain("you are the leader");
		node.close();
		installElectionForTests(null);
	});
});
