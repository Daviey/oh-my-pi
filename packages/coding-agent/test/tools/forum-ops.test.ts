import { beforeEach, describe, expect, it } from "bun:test";
import type { ForumFrame } from "../../src/irc/remote/protocol";
import { executeForumPost, executeForumRead, ingestForumFrame, resetForumForTests } from "../../src/irc/messaging";

describe("forum ops (agent-facing)", () => {
	beforeEach(() => {
		resetForumForTests();
	});

	it("read returns null-ish history (empty) for an untouched channel via ingest path", () => {
		ingestForumFrame({ kind: "forum", channel: "triage", from: "a", body: "hello", ts: 1 });
		expect(executeForumRead("other")).toEqual([]);
		expect(executeForumRead("triage")).toEqual([{ kind: "forum", channel: "triage", from: "a", body: "hello", ts: 1 }]);
	});

	it("ingest buffers frames newest-last and caps at the ring limit", () => {
		for (let i = 0; i < 205; i++) {
			ingestForumFrame({ kind: "forum", channel: "flood", from: "a", body: `m${i}`, ts: i });
		}
		const log = executeForumRead("flood")!;
		expect(log).toHaveLength(200);
		expect(log[0]!.body).toBe("m5"); // oldest 5 evicted
		expect(log.at(-1)!.body).toBe("m204");
	});

	it("post without a hub client errors cleanly, never throws", async () => {
		const result = await executeForumPost(
			{ registry: undefined as never, senderId: "Main" },
			{ channel: "triage", message: "hi" },
		);
		expect(result.isError).toBe(true);
		expect(result.content[0]).toMatchObject({ type: "text" });
	});
});
