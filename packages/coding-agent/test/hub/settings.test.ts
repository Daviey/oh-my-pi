import { describe, expect, it } from "bun:test";
import { resolveHubSocketPath, resolveHubTransport } from "@oh-my-pi/pi-coding-agent/hub/settings";

describe("hub transport settings", () => {
	describe("resolveHubTransport truth table", () => {
		it("unix is implemented", () => {
			expect(resolveHubTransport("unix")).toEqual({ kind: "unix", implemented: true });
		});
		it("mqtt is implemented; redis stays declared-only", () => {
			expect(resolveHubTransport("mqtt")).toEqual({ kind: "mqtt", implemented: true });
			expect(resolveHubTransport("redis")).toEqual({ kind: "redis", implemented: false });
		});
		it("trims and case-folds configured values", () => {
			expect(resolveHubTransport("  UNIX ")).toEqual({ kind: "unix", implemented: true });
			expect(resolveHubTransport("MQTT")).toEqual({ kind: "mqtt", implemented: true });
		});
		it("unknown values fail CLOSED on the declared kind — never silent unix", () => {
			const resolved = resolveHubTransport("carrier-pigeon");
			expect(resolved.kind).toBe("carrier-pigeon");
			expect(resolved.implemented).toBe(false);
		});
		it("empty string fails closed (callers default before calling)", () => {
			expect(resolveHubTransport("").implemented).toBe(false);
		});
	});

	describe("resolveHubSocketPath", () => {
		it("explicit path wins", () => {
			expect(resolveHubSocketPath("/tmp/x.sock", "/agent")).toBe("/tmp/x.sock");
		});
		it("empty derives agentDir/hub.sock", () => {
			expect(resolveHubSocketPath("", "/agent")).toBe("/agent/hub.sock");
			expect(resolveHubSocketPath("   ", "/agent")).toBe("/agent/hub.sock");
		});
	});
});

import { redactHubUrl } from "@oh-my-pi/pi-coding-agent/hub/settings";

describe("redactHubUrl", () => {
	it("strips userinfo credentials", () => {
		expect(redactHubUrl("mqtt://user:pass@pico:1883")).toBe("mqtt://pico:1883");
	});
	it("leaves clean URLs untouched", () => {
		expect(redactHubUrl("mqtt://pico:1883")).toBe("mqtt://pico:1883");
	});
	it("handles unparseable input without leaking userinfo", () => {
		expect(redactHubUrl("garbage//user:pass@host/path")).toBe("garbage//host/path");
	});
});
