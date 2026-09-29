import { describe, expect, it } from "bun:test";
import { parseInternalUrl } from "../../src/internal-urls/parse";
import { AgentProtocolHandler } from "../../src/internal-urls/agent-protocol";
import { peerDirectory } from "../../src/irc/messaging";

describe("agent://peers", () => {
	it("peerDirectory returns null when the hub is disabled (test env)", async () => {
		// No hub config in tests: isHubEnabled() false → null per contract,
		// NOT {peers: []} (which is reserved for hub-up-no-peers).
		expect(await peerDirectory()).toBeNull();
	});

	it("resolve throws the hub-unavailable error when disabled", async () => {
		const handler = new AgentProtocolHandler();
		const url = parseInternalUrl("agent://peers");
		let threw: unknown;
		try {
			await handler.resolve(url);
		} catch (err) {
			threw = err;
		}
		expect(threw).toBeInstanceOf(Error);
		expect((threw as Error).message).toContain("hub unavailable");
	});

	it("locate stays null for peers (virtual resource, not file-backed)", async () => {
		const handler = new AgentProtocolHandler();
		const url = parseInternalUrl("agent://peers");
		expect(await handler.locate(url)).toBeNull();
	});
});
