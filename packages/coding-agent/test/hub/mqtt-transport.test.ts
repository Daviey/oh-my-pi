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
import { resolveHubArea } from "@oh-my-pi/pi-coding-agent/hub/settings";
import type { HubAgentIdentity } from "@oh-my-pi/pi-coding-agent/irc/remote/client";
import type { HubClientFrame, HubServerFrame } from "@oh-my-pi/pi-coding-agent/irc/remote/protocol";

/** In-memory fake broker client: records publishes, replays injected frames. */
class FakeMqttClient implements MqttLikeClient {
	published: { topic: string; payload: string; qos?: number; retain?: boolean }[] = [];
	subscribed: { topic: string; qos?: number }[] = [];
	ended = false;
	endForced: boolean[] = [];
	#messageHandlers: ((topic: string, payload: Buffer) => void)[] = [];
	#closeHandlers: (() => void)[] = [];
	#errorHandlers: ((err: Error) => void)[] = [];

	constructor(readonly connectArgs: MqttConnectArgs) {}

	async endAsync(force?: boolean): Promise<void> {
		this.ended = true;
		this.endForced.push(force ?? false);
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

	async subscribeAsync(topic: string | string[], opts?: { qos?: number }): Promise<unknown> {
		for (const one of Array.isArray(topic) ? topic : [topic]) {
			this.subscribed.push({ topic: one, qos: opts?.qos });
		}
		return undefined;
	}

	on(event: "message", cb: (topic: string, payload: Buffer) => void): void;
	on(event: "close", cb: () => void): void;
	on(event: "error", cb: (err: Error) => void): void;
	on(event: string, cb: never | ((topic: string, payload: Buffer) => void) | (() => void) | ((err: Error) => void)): void {
		if (event === "message") this.#messageHandlers.push(cb as (topic: string, payload: Buffer) => void);
		else if (event === "close") this.#closeHandlers.push(cb as () => void);
		else if (event === "error") this.#errorHandlers.push(cb as (err: Error) => void);
	}

	/** Test-side injection: deliver a publish from the broker. */
	deliver(topic: string, payload: string | Buffer): void {
		const buffer = typeof payload === "string" ? Buffer.from(payload, "utf8") : payload;
		for (const handler of [...this.#messageHandlers]) handler(topic, buffer);
	}

	/** Test-side injection: broker dropped the connection. */
	simulateClose(): void {
		for (const handler of [...this.#closeHandlers]) handler();
	}

	lastFrames(): (HubClientFrame | HubServerFrame)[] {
		return this.published
			.filter(entry => entry.topic.startsWith("hub/") && entry.topic.endsWith("/frames"))
			.flatMap(entry => entry.payload.split("\n").filter(Boolean))
			.map(line => JSON.parse(line) as HubClientFrame | HubServerFrame);
	}

}

/** Connected fakes created by the installed factory (for assertions/teardown). */
const fakes: FakeMqttClient[] = [];
/** Factory calls seen (null factory result = broker refusal). */
const connectCalls: (MqttConnectArgs | null)[] = [];
/** Per-test factory result override. */
let nextResult: MqttConnectResult | undefined;

function installFactory(): void {
	setMqttConnectFactory(args => {
		connectCalls.push(args);
		if (nextResult !== undefined) return Promise.resolve(nextResult);
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
	connectCalls.length = 0;
	fakes.length = 0;
	for (const key of ENV_VARS) {
		if (savedEnv[key] === undefined) delete process.env[key];
		else process.env[key] = savedEnv[key];
	}
});

describe("mqtt hub transport", () => {
	it("missing credentials: connect resolves null and never touches the broker", async () => {
		const client = await MqttHubClient.connect({ url: "mqtt://broker.local:1883", area: "team", identity: identity() });
		expect(client).toBeNull();
		expect(connectCalls).toEqual([]);
	});

	it("userinfo URL credentials are accepted (no env vars needed)", async () => {
		const client = await MqttHubClient.connect({
			url: "mqtt://alice:secret@broker.local:1883",
			area: "team",
			identity: identity(),
		});
		expect(client).not.toBeNull();
		expect(connectCalls.length).toBe(1);
		expect(connectCalls[0]!.username).toBe("alice");
		expect(connectCalls[0]!.password).toBe("secret");
		client!.close();
	});

	it("env-var credentials are accepted", async () => {
		process.env.OMP_HUB_MQTT_USERNAME = "bob";
		process.env.OMP_HUB_MQTT_PASSWORD = "hunter2";
		const client = await MqttHubClient.connect({
			url: "mqtt://broker.local:1883",
			area: "team",
			identity: identity(),
		});
		expect(client).not.toBeNull();
		expect(connectCalls[0]!.username).toBe("bob");
		expect(connectCalls[0]!.password).toBe("hunter2");
		client!.close();
	});

	it("broker refusal resolves null", async () => {
		process.env.OMP_HUB_MQTT_USERNAME = "bob";
		nextResult = null;
		const client = await MqttHubClient.connect({
			url: "mqtt://broker.local:1883",
			area: "team",
			identity: identity(),
		});
		expect(client).toBeNull();
	});

	it("topics are namespaced by area; empty area falls back to default", async () => {
		const explicit = await MqttHubClient.connect({
			url: "mqtt://u:p@broker.local:1883",
			area: "  fleet-a  ",
			identity: identity(),
		});
		expect(explicit!.area).toBe("fleet-a");
		const fakeA = fakes[0]!;
		expect(fakeA.subscribed.map(entry => entry.topic)).toContain("hub/fleet-a/frames");
		expect(fakeA.subscribed.map(entry => entry.topic)).toContain("hub/fleet-a/presence/+");
		expect(fakeA.published.some(entry => entry.topic === "hub/fleet-a/presence/Main:4242" && entry.retain)).toBe(true);
		expect(fakeA.published.filter(entry => entry.topic === "hub/fleet-a/frames").every(entry => entry.qos === 1)).toBe(true);

		const fallback = await MqttHubClient.connect({
			url: "mqtt://u:p@broker.local:1883",
			area: "   ",
			identity: identity(),
		});
		expect(fallback!.area).toBe("default");
		const fakeB = fakes[1]!;
		expect(fakeB.subscribed.map(entry => entry.topic)).toContain("hub/default/frames");
		expect(fakeB.published.some(entry => entry.topic === "hub/default/presence/Main:4242")).toBe(true);

		explicit!.close();
		fallback!.close();
	});

	it("client id is omp-<area>-<agentId>-<pid>", async () => {
		const client = await MqttHubClient.connect({
			url: "mqtt://u:p@broker.local:1883",
			area: "team",
			identity: identity({ agentId: "Worker", pid: 99 }),
		});
		expect(connectCalls[0]!.clientId).toBe("omp-team-Worker-99");
		client!.close();
	});

	it("presence is retained on connect and cleared on close", async () => {
		const client = await MqttHubClient.connect({
			url: "mqtt://u:p@broker.local:1883",
			area: "team",
			identity: identity(),
		});
		const fake = fakes[0]!;
		const retainedRow = fake.published.find(entry => entry.topic === "hub/team/presence/Main:4242");
		expect(retainedRow?.retain).toBe(true);
		expect(JSON.parse(retainedRow!.payload).agentId).toBe("Main");

		client!.close();
		await Bun.sleep(10); // close() clears presence asynchronously
		const cleared = fake.published.filter(entry => entry.topic === "hub/team/presence/Main:4242").pop();
		expect(cleared!.payload).toBe("");
		expect(cleared!.retain).toBe(true);
		expect(fake.ended).toBe(true);
	});

	it("retained presence snapshots seed the roster; clears remove them", async () => {
		const client = await MqttHubClient.connect({
			url: "mqtt://u:p@broker.local:1883",
			area: "team",
			identity: identity(),
		});
		const fake = fakes[0]!;
		fake.deliver(
			hubPresenceTopic("team", "Worker", 7),
			JSON.stringify({ agentId: "Worker", project: "proj-b", status: "idle", pid: 7 }),
		);
		let roster = await client!.roster();
		expect(roster.map(entry => entry.agentId)).toEqual(["Worker"]);

		// Self-echo of own registration never enters the roster.
		fake.deliver(
			hubPresenceTopic("team", "Main", 4242),
			JSON.stringify(identity()),
		);
		roster = await client!.roster();
		expect(roster.map(entry => entry.agentId)).toEqual(["Worker"]);

		fake.deliver(hubPresenceTopic("team", "Worker", 7), Buffer.alloc(0));
		roster = await client!.roster();
		expect(roster).toEqual([]);
		client!.close();
	});

	it("delivery filtering: wrong agentId not delivered, right one delivered, pid narrows", async () => {
		const me = identity({ agentId: "Main", pid: 4242 });
		const client = await MqttHubClient.connect({ url: "mqtt://u:p@broker.local:1883", area: "team", identity: me });
		const fake = fakes[0]!;
		const delivered: string[] = [];
		client!.onDelivery(msg => delivered.push(msg.body));

		// Wrong agentId: not for us.
		fake.deliver(hubFramesTopic("team"), `${JSON.stringify({ type: "publish", msg: { id: "m1", from: "a", to: "b", body: "nope", ts: 1 }, targets: [{ agentId: "Worker" }] })}\n`);
		expect(delivered).toEqual([]);

		// Right agentId, no pid: delivered.
		fake.deliver(hubFramesTopic("team"), `${JSON.stringify({ type: "publish", msg: { id: "m2", from: "a", to: "Main", body: "hello", ts: 2 }, targets: [{ agentId: "Main" }] })}\n`);
		expect(delivered).toEqual(["hello"]);

		// Right agentId but pid of a different process: not delivered.
		fake.deliver(hubFramesTopic("team"), `${JSON.stringify({ type: "publish", msg: { id: "m3", from: "a", to: "Main", body: "other pid", ts: 3 }, targets: [{ agentId: "Main", pid: 9999 }] })}\n`);
		expect(delivered).toEqual(["hello"]);

		// pid narrowing matches this exact process.
		fake.deliver(hubFramesTopic("team"), `${JSON.stringify({ type: "publish", msg: { id: "m4", from: "a", to: "Main", body: "exact pid", ts: 4 }, targets: [{ agentId: "Main", pid: 4242 }] })}\n`);
		expect(delivered).toEqual(["hello", "exact pid"]);
		client!.close();
	});

	it("publish fans out on the frames topic and matching peers ack by frame id", async () => {
		const client = await MqttHubClient.connect({ url: "mqtt://u:p@broker.local:1883", area: "team", identity: identity() });
		const fake = fakes[0]!;

		const pending = client!.publish({ id: "req-1", from: "Main", to: "Worker", body: "ping", ts: 1 }, [
			{ agentId: "Worker" },
		]);
		await Bun.sleep(10);
		const publishFrame = fake.lastFrames().find(frame => frame.type === "publish");
		expect(publishFrame).toBeDefined();

		// The Worker peer (not us) answers with an ack correlated by frame id.
		fake.deliver(
			hubFramesTopic("team"),
			`${JSON.stringify({ type: "publishAck", id: "req-1", results: [{ to: "Worker", ok: true }] })}\n`,
		);
		const result = await pending;
		expect(result).toEqual({ results: [{ to: "Worker", ok: true }] });

		// Self-echo of our own publish frame must not deliver to ourselves.
		const delivered: string[] = [];
		client!.onDelivery(msg => delivered.push(msg.body));
		fake.deliver(hubFramesTopic("team"), `${JSON.stringify(publishFrame)}\n`);
		expect(delivered).toEqual([]);
		client!.close();
	});

	it("publish with no matching peer resolves null on timeout", async () => {
		const client = await MqttHubClient.connect({ url: "mqtt://u:p@broker.local:1883", area: "team", identity: identity() });
		const result = await client!.publish({ id: "req-2", from: "Main", to: "Ghost", body: "anyone?", ts: 2 }, [
			{ agentId: "Ghost" },
		]);
		expect(result).toBeNull();
		client!.close();
	}, 5_000);

	it("receive-side matching acks the sender back", async () => {
		const me = identity({ agentId: "Main", pid: 4242 });
		const client = await MqttHubClient.connect({ url: "mqtt://u:p@broker.local:1883", area: "team", identity: me });
		const fake = fakes[0]!;
		client!.onDelivery(() => {});

		// Another peer's publish addressed to us (frame id unknown to us → not self-echo).
		fake.deliver(
			hubFramesTopic("team"),
			`${JSON.stringify({ type: "publish", msg: { id: "peer-1", from: "Worker", to: "Main", body: "hi", ts: 5 }, targets: [{ agentId: "Main" }] })}\n`,
		);
		await Bun.sleep(10);
		const ack = fake.lastFrames().find(frame => frame.type === "publishAck");
		expect(ack).toEqual({ type: "publishAck", id: "peer-1", results: [{ to: "Main", ok: true }] });
		client!.close();
	});

	it("setStatus republishes retained presence with the new status", async () => {
		const client = await MqttHubClient.connect({ url: "mqtt://u:p@broker.local:1883", area: "team", identity: identity() });
		const fake = fakes[0]!;
		await client!.setStatus("idle", "waiting on tests");
		const row = fake.published.filter(entry => entry.topic === "hub/team/presence/Main:4242").pop();
		expect(row!.retain).toBe(true);
		expect(JSON.parse(row!.payload).status).toBe("idle");
		expect(JSON.parse(row!.payload).activity).toBe("waiting on tests");
		client!.close();
	});

	it("broker close invokes the onClose handler", async () => {
		const client = await MqttHubClient.connect({ url: "mqtt://u:p@broker.local:1883", area: "team", identity: identity() });
		let closed = 0;
		client!.onClose(() => closed++);
		fakes[0]!.simulateClose();
		expect(closed).toBe(1);
		client!.close();
	});
});

describe("resolveHubArea truth table", () => {
	it("trims whitespace", () => {
		expect(resolveHubArea("  fleet  ")).toBe("fleet");
	});
	it("empty and whitespace-only fall back to default", () => {
		expect(resolveHubArea("")).toBe("default");
		expect(resolveHubArea("   ")).toBe("default");
		expect(resolveHubArea("\t\n")).toBe("default");
	});
	it("explicit value wins over default", () => {
		expect(resolveHubArea("prod")).toBe("prod");
	});
});

describe("topic helpers", () => {
	it("frames topic is area-namespaced", () => {
		expect(hubFramesTopic("fleet")).toBe("hub/fleet/frames");
	});
	it("presence topic is area + agentId namespaced", () => {
		expect(hubPresenceTopic("fleet", "Main", 4242)).toBe("hub/fleet/presence/Main:4242");
	});
});

