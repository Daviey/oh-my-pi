import { describe, expect, it } from "bun:test";
import { parseSystemScopeForTest } from "@oh-my-pi/pi-coding-agent/irc/messaging";

describe("system/global scope syntax", () => {
	it("parses system:<id>", () => {
		expect(parseSystemScopeForTest("system:Main")).toEqual({ agentId: "Main" });
	});
	it("parses system:all broadcast", () => {
		expect(parseSystemScopeForTest("system:all")).toEqual({ agentId: "all" });
	});
	it("global is an alias for system (single-host era)", () => {
		expect(parseSystemScopeForTest("global:Main")).toEqual({ agentId: "Main" });
		expect(parseSystemScopeForTest("global:all")).toEqual({ agentId: "all" });
	});
	it("does not swallow project-scoped or bare ids", () => {
		expect(parseSystemScopeForTest("project:abc123:Main")).toBeNull();
		expect(parseSystemScopeForTest("Main")).toBeNull();
		expect(parseSystemScopeForTest("all")).toBeNull();
	});
	it("requires an id after the prefix", () => {
		expect(parseSystemScopeForTest("system:")).toBeNull();
		expect(parseSystemScopeForTest("global:")).toBeNull();
	});
});
