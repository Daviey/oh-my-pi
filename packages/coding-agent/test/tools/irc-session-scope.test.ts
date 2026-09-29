import { describe, expect, it } from "bun:test";
import {
	diagnoseProjectNsForTest,
	parseSessionScopeForTest,
	parseSystemScopeForTest,
	resolveProjectNsForTest,
} from "../../src/irc/messaging";

describe("parseSessionScope", () => {
	it("parses a bare session id", () => {
		expect(parseSessionScopeForTest("session:01a0e13e-9c76-7577-ac24-2a1508d11770")).toEqual({
			sessionId: "01a0e13e-9c76-7577-ac24-2a1508d11770",
			agentId: "Main",
		});
	});

	it("accepts the uniform :peerId suffix without swallowing it into the sessionId", () => {
		// Regression: the suffix used to be absorbed into the sessionId,
		// producing "no session \"Main\" with sessionId <uuid>:Main".
		expect(parseSessionScopeForTest("session:01a0e13e-9c76-7577-ac24-2a1508d11770:Main")).toEqual({
			sessionId: "01a0e13e-9c76-7577-ac24-2a1508d11770",
			agentId: "Main",
		});
	});

	it("accepts a non-Main peerId suffix", () => {
		expect(parseSessionScopeForTest("session:abc123:0-Sub")).toEqual({
			sessionId: "abc123",
			agentId: "0-Sub",
		});
	});

	it("rejects non-session scopes", () => {
		expect(parseSessionScopeForTest("pid:123:Main")).toBeNull();
		expect(parseSessionScopeForTest("system:Main")).toBeNull();
		expect(parseSessionScopeForTest("Main")).toBeNull();
	});

	it("system scope unaffected", () => {
		expect(parseSystemScopeForTest("system:Main")).toEqual({ agentId: "Main" });
	});
});

describe("resolveProjectNs alias resolution", () => {
	const roster = [
		{ project: "aaaa1111aaaa1111", specialism: "vixie-hq" },
		{ project: "bbbb2222bbbb2222", specialism: "autoreview" },
		{ project: "bbbb2222bbbb2223", specialism: "autoreview-two" },
	];

	it("exact hash wins outright", () => {
		expect(resolveProjectNsForTest(roster, "aaaa1111aaaa1111")).toEqual(["aaaa1111aaaa1111"]);
	});

	it("unique specialism resolves to its hash", () => {
		expect(resolveProjectNsForTest(roster, "vixie-hq")).toEqual(["aaaa1111aaaa1111"]);
	});

	it("exact specialism beats a prefix family — resolves, not ambiguous", () => {
		// "autoreview" is a unique specialism even though "autoreview-two"
		// startsWith it: specialism matches are exact, so they win before the
		// prefix family is consulted. (Pinned after a probe showed the original
		// ambiguity assumption was wrong.)
		expect(resolveProjectNsForTest(roster, "autoreview")).toEqual(["bbbb2222bbbb2222"]);
		expect(diagnoseProjectNsForTest(roster, "autoreview")).toBeNull();
	});

	it("ambiguous hash prefix resolves to nothing and lists candidates", () => {
		// "bbbb2" prefixes two registered hashes: ambiguity is an error, never
		// a guess — and the diagnosis names both so the caller can pick.
		expect(resolveProjectNsForTest(roster, "bbbb2")).toEqual([]);
		expect(diagnoseProjectNsForTest(roster, "bbbb2")).toEqual(["bbbb2222bbbb2222", "bbbb2222bbbb2223"]);
	});

	it("unique hash prefix resolves", () => {
		expect(resolveProjectNsForTest(roster, "aaaa")).toEqual(["aaaa1111aaaa1111"]);
	});

	it("unknown token: no match and no ambiguity diagnosis", () => {
		expect(resolveProjectNsForTest(roster, "zzzz")).toEqual([]);
		expect(diagnoseProjectNsForTest(roster, "zzzz")).toBeNull();
	});
});
