import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import {
	hubFramesTopic,
	hubPresenceTopic,
	type MqttConnectArgs,
	type MqttConnectResult,
	type MqttLikeClient,
	MqttHubClient,
	setMqttConnectFactory,
} from "@oh-my-pi/pi-coding-agent/irc/remote/mqtt";
import type { HubAgentIdentity } from "@oh-my-pi/pi-coding-agent/irc/remote/client";
import type {
	HubClientFrame,
	HubServerFrame,
} from "@oh-my-pi/pi-coding-agent/irc/remote/protocol";

/**
 * Regression tests for the hub MQTT transport's long-lived-process decay:
 * request/delivery sinks must survive broker reconnects, a throwing handler
 * must produce a fast error reply (not a silent 120s hang), and presence must
 * be keyed by (agentId, pid) so a same-id peer's exit clear cannot wipe the
 * roster row of the process still running.
 */

/** In-memory fake broker client: records publishes, replays injected frames. */
class FakeMqttClient implements MqttLikeClient {
	published: {
		topic: string;
		payload: string;
		qos?: number;
		retain?: boolean;
	}[] = [];
	subscribed: { topic: string; qos?: number }[] = [];
	ended = false;
	endForced: boolean[] = [];
	#messageHandlers: ((topic: string, payload: Buffer) => void)[] = [];
	#closeHandlers: (() => void)[] = [];
	#errorHandlers: ((err: Error) => void)[] = [];

	constructor(readonly connectArgs: MqttConnectArgs) {}

	async endAsync(force?: boolean): Promise<void> {
		this.ended = true;
		if (force !== undefined) this.endForced.push(force);
	}

	async publishAsync(
		topic: string,
		message: string | Buffer,
		opts?: { qos?: number; retain?: boolean },
	): Promise<unknown> {
		this.published.push({
			topic,
			payload: typeof message === "string" ? message : message.toString("utf8"),
			qos: opts?.qos,
			retain: opts?.retain,
		});
		return undefined;
	}

	async subscribeAsync(
		topic: string | string[],
		opts?: { qos?: number },
	): Promise<unknown> {
		for (const entry of typeof topic === "string" ? [topic] : topic) {
			this.subscribed.push({ topic: entry, qos: opts?.qos });
		}
		return undefined;
	}

	on(event: "message", cb: (topic: string, payload: Buffer) => void): void;
	on(event: "close", cb: () => void): void;
	on(event: "error", cb: (err: Error) => void): void;
	on(
		event: string,
		cb:
			| never
			| ((topic: string, payload: Buffer) => void)
			| (() => void)
			| ((err: Error) => void),
	): void {
		if (event === "message")
			this.#messageHandlers.push(
				cb as (topic: string, payload: Buffer) => void,
			);
		else if (event === "close") this.#closeHandlers.push(cb as () => void);
		else if (event === "error")
			this.#errorHandlers.push(cb as (err: Error) => void);
	}

	/** Test-side injection: deliver a publish from the broker. */
	deliver(topic: string, payload: string | Buffer): void {
		const buffer =
			typeof payload === "string" ? Buffer.from(payload, "utf8") : payload;
		for (const handler of [...this.#messageHandlers]) handler(topic, buffer);
	}

	/** Test-side injection: broker dropped the connection. */
	simulateClose(): void {
		for (const handler of [...this.#closeHandlers]) handler();
	}

	/** Decoded frames this fake published on the area's frames topic. */
	lastFrames(): (HubClientFrame | HubServerFrame)[] {
		return this.published
			.filter(
				(entry) =>
					entry.topic.startsWith("hub/") && entry.topic.endsWith("/frames"),
			)
			.flatMap((entry) => entry.payload.split("\n").filter(Boolean))
			.map((line) => JSON.parse(line) as HubClientFrame | HubServerFrame);
	}
}

/** Connected fakes created by the installed factory. */
const fakes: FakeMqttClient[] = [];

/** Factory result override for the next connect (null = broker refusal). */
let nextResult: MqttConnectResult | undefined;

function installFactory(): void {
	setMqttConnectFactory((args) => {
		if (nextResult !== undefined) {
			const result = nextResult;
			nextResult = undefined;
			return Promise.resolve(result);
		}
		const fake = new FakeMqttClient(args);
		fakes.push(fake);
		return Promise.resolve(fake);
	});
}

function identity(overrides: Partial<HubAgentIdentity> = {}): HubAgentIdentity {
	return {
		agentId: "Main",
		project: "proj-a",
		status: "running",
		pid: 4242,
		...overrides,
	};
}

const ENV_VARS = ["OMP_HUB_MQTT_USERNAME", "OMP_HUB_MQTT_PASSWORD"] as const;
const savedEnv: Record<string, string | undefined> = {};

beforeEach(() => {
	for (const key of ENV_VARS) savedEnv[key] = process.env[key];
	delete process.env.OMP_HUB_MQTT_USERNAME;
	delete process.env.OMP_HUB_MQTT_PASSWORD;
	installFactory();
});

afterEach(() => {
	setMqttConnectFactory(null);
	nextResult = undefined;
	fakes.length = 0;
	for (const key of ENV_VARS) {
		if (savedEnv[key] === undefined) delete process.env[key];
		else process.env[key] = savedEnv[key];
	}
});

describe("mqtt hub transport regression: sinks and presence lifecycle", () => {
	it("presence clear from a same-id peer removes only that peer's roster row", async () => {
		// Two remote processes share agentId "Main" (every process's main
		// agent is "Main"): each owns a (agentId, pid)-keyed presence slot.
		const me = identity({ agentId: "Answerer", pid: 1 });
		const client = await MqttHubClient.connect({
			url: "mqtt://u:p@broker.local:1883",
			area: "team",
			identity: me,
		});
		expect(client).not.toBeNull();
		const fake = fakes[0]!;

		const peerA = identity({ agentId: "Main", pid: 100 });
		const peerB = identity({ agentId: "Main", pid: 200 });
		fake.deliver(hubPresenceTopic("team", "Main", 100), JSON.stringify(peerA));
		fake.deliver(hubPresenceTopic("team", "Main", 200), JSON.stringify(peerB));
		let roster = await client!.roster();
		expect(roster).toHaveLength(2);
		expect(new Set(roster.map((entry) => entry.pid))).toEqual(
			new Set([100, 200]),
		);

		// Peer A exits cleanly: its empty-payload clear must remove ONLY its
		// own row. On the agentId-keyed scheme this wiped peer B's row too.
		fake.deliver(hubPresenceTopic("team", "Main", 100), Buffer.alloc(0));
		roster = await client!.roster();
		expect(roster).toHaveLength(1);
		expect(roster[0]!.pid).toBe(200);

		// The LWT (empty retained payload on the dying process's own topic)
		// clears exactly the same slot.
		fake.deliver(hubPresenceTopic("team", "Main", 200), Buffer.alloc(0));
		roster = await client!.roster();
		expect(roster).toEqual([]);
		client!.close();
	});

	it("presence topics are per-process: same agentId, distinct pids, distinct slots", async () => {
		expect(hubPresenceTopic("team", "Main", 100)).toBe(
			"hub/team/presence/Main:100",
		);
		expect(hubPresenceTopic("team", "Main", 200)).toBe(
			"hub/team/presence/Main:200",
		);
		// The single-level wildcard subscription still covers the pid-suffixed
		// segment (no extra topic depth was introduced).
		const me = identity({ agentId: "Main", pid: 4242 });
		const client = await MqttHubClient.connect({
			url: "mqtt://u:p@broker.local:1883",
			area: "team",
			identity: me,
		});
		const fake = fakes[0]!;
		expect(fake.subscribed.map((entry) => entry.topic)).toContain(
			"hub/team/presence/+",
		);
		const own = fake.published.filter(
			(entry) => entry.topic === "hub/team/presence/Main:4242",
		);
		expect(own.length).toBeGreaterThan(0);
		expect(own.every((entry) => entry.retain === true)).toBe(true);
		client!.close();
	});

	it("a throwing request handler answers with an error reply, not silence", async () => {
		const me = identity({ agentId: "Main", pid: 4242 });
		const client = await MqttHubClient.connect({
			url: "mqtt://u:p@broker.local:1883",
			area: "team",
			identity: me,
		});
		const fake = fakes[0]!;
		// The reply rides publishAsync (async dispatch): await the publish
		// signal itself instead of guessing a sleep duration.
		const replied = Promise.withResolvers<void>();
		const published = fake.publishAsync.bind(fake);
		fake.publishAsync = async (topic, message, opts) => {
			const result = await published(topic, message, opts);
			replied.resolve();
			return result;
		};
		client!.onRequest(() => {
			throw new Error("recipient session is disposed");
		});

		// A peer's request addressed to us (frame id unknown to us → not self-echo).
		fake.deliver(
			hubFramesTopic("team"),
			`${JSON.stringify({ type: "request", id: "rpc-1", msg: { id: "rpc-1", from: "Peer", to: "Main", body: "ping", ts: 1 }, targets: [{ agentId: "Main" }] })}\n`,
		);
		await replied.promise;

		// Pre-fix behavior: the throw was swallowed into `answer = null` and
		// NO reply frame was published — the requester hung for its full RPC
		// timeout (120s by default). Now a fast error reply rides the frames topic.
		const reply = fake.lastFrames().find((frame) => frame.type === "reply");
		expect(reply).toBeDefined();
		expect(reply).toMatchObject({ type: "reply", id: "rpc-1", from: "Main" });
		const body = (reply as { msg?: { body?: string; replyTo?: string } }).msg;
		expect(body?.replyTo).toBe("rpc-1");
		expect(body?.body).toContain("rpc-error");
		expect(body?.body).toContain("recipient session is disposed");
		client!.close();
	});

	it("reconnect ladder re-attaches sinks: a fresh client instance still delivers and answers", async () => {
		// H-A trace shape: when the broker connection drops, hub-manager's
		// ladder builds a NEW MqttHubClient and re-attaches the bus sinks via
		// IrcBus.attachHubClient. This pins that contract at the transport
		// seam: a second client created through the same factory path (as the
		// retry ladder does) carries the freshly registered sinks — a delivery
		// reaches the delivery sink and a request reaches the handler, and the
		// handler's answer round-trips as a correlated reply frame.
		const first = await MqttHubClient.connect({
			url: "mqtt://u:p@broker.local:1883",
			area: "team",
			identity: identity(),
		});
		expect(first).not.toBeNull();
		const deliveredBefore: string[] = [];
		first!.onDelivery((msg) => deliveredBefore.push(msg.body));
		first!.onRequest(() => ({
			id: "old",
			from: "Main",
			to: "Peer",
			body: "stale",
			ts: 0,
		}));

		// Broker drops the connection: onClose fires (hub-manager drops its
		// cache and schedules the retry), then the ladder reconnects — a new
		// instance through the same factory.
		fakes[0]!.simulateClose();
		const second = await MqttHubClient.connect({
			url: "mqtt://u:p@broker.local:1883",
			area: "team",
			identity: identity(),
		});
		expect(second).not.toBeNull();
		expect(second).not.toBe(first);
		// What hub-manager does on reconnect: attach the bus sinks to the new client.
		const delivered: string[] = [];
		let handledFrom = "";
		const answered = Promise.withResolvers<void>();
		second!.onDelivery((msg) => delivered.push(msg.body));
		second!.onRequest((msg, from) => {
			handledFrom = from;
			return {
				id: `reply-${msg.id}`,
				from: "Main",
				to: from || "Peer",
				body: "fresh answer",
				ts: Date.now(),
			};
		});
		// The reply frame rides publishAsync: await that signal, not a sleep.
		const fake2 = fakes[1]!;
		const published2 = fake2.publishAsync.bind(fake2);
		fake2.publishAsync = async (topic, message, opts) => {
			const result = await published2(topic, message, opts);
			if (typeof message === "string" && message.includes('"reply"'))
				answered.resolve();
			return result;
		};

		// A peer publish addressed to us must reach the delivery sink of the
		// instance actually receiving frames post-reconnect.
		fake2.deliver(
			hubFramesTopic("team"),
			`${JSON.stringify({ type: "publish", msg: { id: "post-reconnect-1", from: "Peer", to: "Main", body: "hello after reconnect", ts: 2 }, targets: [{ agentId: "Main" }] })}\n`,
		);
		expect(delivered).toEqual(["hello after reconnect"]);

		// A peer request must reach the handler and its answer must come back
		// as a correlated reply frame on the new instance.
		fake2.deliver(
			hubFramesTopic("team"),
			`${JSON.stringify({ type: "request", id: "rpc-2", msg: { id: "rpc-2", from: "Peer", to: "Main", body: "question", ts: 3 }, targets: [{ agentId: "Main" }] })}\n`,
		);
		await answered.promise;
		expect(handledFrom).toBe("Peer");
		const reply = fakes[1]!
			.lastFrames()
			.find((frame) => frame.type === "reply");
		expect(reply).toMatchObject({ type: "reply", id: "rpc-2", from: "Main" });
		expect((reply as { msg?: { body?: string } }).msg?.body).toBe(
			"fresh answer",
		);
		first!.close();
		second!.close();
	}, 10_000);
});
