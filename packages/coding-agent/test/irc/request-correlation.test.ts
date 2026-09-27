import { describe, expect, it } from "bun:test";
import { IrcBus } from "@oh-my-pi/pi-coding-agent/irc/bus";

/** Seed a mailbox without registry/session machinery: redeliver is the
 *  raw mailbox-insert primitive, so these tests exercise exactly the
 *  takeMatching splice semantics. */
describe("IrcBus.takeMatching", () => {
	it("consumes only the matching message, preserving the rest", () => {
		const bus = new IrcBus();
		// redeliver puts messages at the FRONT (it models putting a taken
		// message back), so seed in reverse arrival order: mailbox = a,b,c.
		bus.redeliver("Main", { id: "c", from: "Z", to: "Main", body: "also unrelated", ts: 3 });
		bus.redeliver("Main", { id: "b", from: "Y", to: "Main", body: "reply", replyTo: "req-1", ts: 2 });
		bus.redeliver("Main", { id: "a", from: "X", to: "Main", body: "unrelated", ts: 1 });
		const reply = bus.takeMatching("Main", m => m.replyTo === "req-1");
		expect(reply?.id).toBe("b");
		// The others remain, in order:
		expect(bus.take("Main")?.id).toBe("a");
		expect(bus.take("Main")?.id).toBe("c");
		expect(bus.take("Main")).toBeUndefined();
	});

	it("returns undefined when nothing matches (mailbox untouched)", () => {
		const bus = new IrcBus();
		bus.redeliver("Main", { id: "a", from: "X", to: "Main", body: "hi", ts: 1 });
		expect(bus.takeMatching("Main", m => m.replyTo === "nope")).toBeUndefined();
		expect(bus.take("Main")?.id).toBe("a");
	});
});
