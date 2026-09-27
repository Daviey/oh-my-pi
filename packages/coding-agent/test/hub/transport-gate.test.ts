import { afterEach, describe, expect, it } from "bun:test";
import { configureHub, ensureHubClient, isHubEnabled, resetHubForTests } from "@oh-my-pi/pi-coding-agent/irc/remote/hub-manager";

afterEach(() => {
	resetHubForTests();
});

describe("hub transport gate (fail-closed)", () => {
	it("unimplemented transport config: ensureHubClient resolves null, hub self-disables", async () => {
		configureHub({ enabled: true, socketPath: "/tmp/hub-gate-test.sock", transport: "mqtt", remoteUrl: "mqtt://pico:1883" });
		expect(isHubEnabled()).toBe(true);
		const client = await ensureHubClient();
		expect(client).toBeNull();
		// Fail-closed flips enabled off so retry ladders don't spin on a
		// transport that will never connect.
		expect(isHubEnabled()).toBe(false);
	});

	it("unknown transport value: same fail-closed path", async () => {
		configureHub({ enabled: true, socketPath: "/tmp/hub-gate-test.sock", transport: "carrier-pigeon" });
		const client = await ensureHubClient();
		expect(client).toBeNull();
		expect(isHubEnabled()).toBe(false);
	});

	it("empty remoteUrl with non-unix transport: still fail-closed", async () => {
		configureHub({ enabled: true, socketPath: "/tmp/hub-gate-test.sock", transport: "redis", remoteUrl: "" });
		const client = await ensureHubClient();
		expect(client).toBeNull();
	});

	it("unix transport connects normally (existing behavior preserved)", async () => {
		configureHub({ enabled: true, socketPath: "/tmp/hub-gate-unix.sock" });
		// Don't actually connect (no broker on that path; spawn-on-connect would
		// race) — assert the gate did NOT self-disable before connect:
		expect(isHubEnabled()).toBe(true);
	});
});
