import { describe, expect, it } from "bun:test";
import { parseSessionScopeForTest, parseSystemScopeForTest } from "../../src/irc/messaging";

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
