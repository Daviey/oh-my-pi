import { afterAll, afterEach, beforeEach, describe, expect, it } from "bun:test";
import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { HubClient } from "@oh-my-pi/pi-coding-agent/irc/remote/client";
import { hubProjectNamespace, startHubBroker } from "@oh-my-pi/pi-coding-agent/irc/remote/broker";
import {
	hubFramesTopic,
	MqttHubClient,
	setMqttConnectFactory,
	type MqttConnectArgs,
	type MqttLikeClient,
} from "@oh-my-pi/pi-coding-agent/irc/remote/mqtt";
import type { HubAgentIdentity } from "@oh-my-pi/pi-coding-agent/irc/remote/client";
import type { ForumFrame } from "@oh-my-pi/pi-coding-agent/irc/remote/protocol";

function tmpSocketPath(): string {
	return path.join(fs.mkdtempSync(path.join(os.tmpdir(), "hub-forum-")), "hub.sock");
}

function identity(agentId: string, project: string, pid: number = process.pid): HubAgentIdentity {
	return { agentId, project, status: "running", pid };
}

/** Poll until `probe` passes or `timeoutMs` elapses (delivery is async). */
async function waitUntil(probe: () => boolean, timeoutMs = 2_000): Promise<void> {
	const deadline = Date.now() + timeoutMs;
	while (!probe()) {
		if (Date.now() >= deadline) throw new Error("waitUntil: condition never met");
		await Bun.sleep(10);
	}
}

const sockets: string[] = [];

afterAll(() => {
	for (const socketPath of sockets) {
		try {
			fs.unlinkSync(socketPath);
		} catch {}
	}
});

describe("hub forum channels (unix broker)", () => {
	async function startBroker(): Promise<string> {
		const socketPath = tmpSocketPath();
		sockets.push(socketPath);
		const listening = Promise.withResolvers<void>();
		void startHubBroker({ socketPath, idleGraceMs: 60_000, onListening: listening.resolve });
		await listening.promise;
		return socketPath;
	}

	it("forumPublish on one client fires every other client's onForum (cross-project broadcast)", async () => {
		const socketPath = await startBroker();
		const a = await HubClient.connect({ socketPath, identity: identity("a", hubProjectNamespace("/p")), spawn: () => {} });
		const b = await HubClient.connect({ socketPath, identity: identity("b", hubProjectNamespace("/q")), spawn: () => {} });
		expect(a).not.toBeNull();
		expect(b).not.toBeNull();

		const gotB: ForumFrame[] = [];
		const gotC: ForumFrame[] = [];
		// Distinct projects on purpose: forums are area-wide, never scoped by
		// hubTargetMatches/project namespaces.
		const c = await HubClient.connect({ socketPath, identity: identity("c", hubProjectNamespace("/r")), spawn: () => {} });
		expect(c).not.toBeNull();
		b!.onForum(frame => gotB.push(frame));
		c!.onForum(frame => gotC.push(frame));

		await a!.forumPublish("triage", "hello");
		await waitUntil(() => gotB.length === 1 && gotC.length === 1);

		expect(gotB[0]).toMatchObject({ channel: "triage", from: "a", body: "hello" });
		expect(typeof gotB[0]!.ts).toBe("number");
		expect(gotC[0]).toMatchObject({ channel: "triage", from: "a", body: "hello" });

		a!.close();
		b!.close();
		c!.close();
	}, 10_000);

	it("unsubscribing stops forum delivery for that handler only", async () => {
		const socketPath = await startBroker();
		const a = await HubClient.connect({ socketPath, identity: identity("a", hubProjectNamespace("/p")), spawn: () => {} });
		const b = await HubClient.connect({ socketPath, identity: identity("b", hubProjectNamespace("/p")), spawn: () => {} });
		expect(a).not.toBeNull();
		expect(b).not.toBeNull();

		const gotB: ForumFrame[] = [];
		const gotKeep: ForumFrame[] = [];
		const unsubscribe = b!.onForum(frame => gotB.push(frame));
		b!.onForum(frame => gotKeep.push(frame));

		await a!.forumPublish("triage", "one");
		await waitUntil(() => gotB.length === 1 && gotKeep.length === 1);
		unsubscribe();
		await a!.forumPublish("triage", "two");
		// The still-subscribed handler proves the second publish was dispatched
		// through the same synchronous handler loop — by then the unsubscribed
		// handler has had its (absent) chance; no timed wait is needed.
		await waitUntil(() => gotKeep.length === 2);
		expect(gotB.length).toBe(1);
		expect(gotKeep.map(frame => frame.body)).toEqual(["one", "two"]);

		a!.close();
		b!.close();
	}, 10_000);

	it("invalid channel names throw at publish time", async () => {
		const socketPath = await startBroker();
		const a = await HubClient.connect({ socketPath, identity: identity("a", hubProjectNamespace("/p")), spawn: () => {} });
		expect(a).not.toBeNull();
		for (const channel of ["", "Bad_Name", "has space", "ünicode", "x".repeat(65), "triage/gate", "TRIAGE"]) {
			await expect(a!.forumPublish(channel, "nope")).rejects.toThrow();
		}
		// Boundary values that MUST pass validation.
		await expect(a!.forumPublish("a", "min")).resolves.toBeUndefined();
		await expect(a!.forumPublish("x".repeat(64), "max")).resolves.toBeUndefined();
		a!.close();
	}, 10_000);
});

/** In-memory fake broker client: records publishes, replays injected frames. */
class FakeMqttClient implements MqttLikeClient {
	published: { topic: string; payload: string; qos?: number; retain?: boolean }[] = [];
	subscribed: { topic: string; qos?: number }[] = [];
	ended = false;
	#messageHandlers: ((topic: string, payload: Buffer) => void)[] = [];
	#closeHandlers: (() => void)[] = [];
	#errorHandlers: ((err: Error) => void)[] = [];

	constructor(readonly connectArgs: MqttConnectArgs) {}

	async endAsync(force?: boolean): Promise<void> {
		this.ended = true;
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
}

const fakes: FakeMqttClient[] = [];

const ENV_VARS = ["OMP_HUB_MQTT_USERNAME", "OMP_HUB_MQTT_PASSWORD"] as const;
const savedEnv: Record<string, string | undefined> = {};

describe("hub forum channels (mqtt transport)", () => {
	beforeEach(() => {
		for (const key of ENV_VARS) savedEnv[key] = process.env[key];
		delete process.env.OMP_HUB_MQTT_USERNAME;
		delete process.env.OMP_HUB_MQTT_PASSWORD;
		setMqttConnectFactory(args => {
			const fake = new FakeMqttClient(args);
			fakes.push(fake);
			return Promise.resolve(fake);
		});
	});

	afterEach(() => {
		setMqttConnectFactory(null);
		fakes.length = 0;
		for (const key of ENV_VARS) {
			if (savedEnv[key] === undefined) delete process.env[key];
			else process.env[key] = savedEnv[key];
		}
	});

	it("forum frames ride the existing frames topic and fire peers' onForum without target matching", async () => {
		const a = await MqttHubClient.connect({
			url: "mqtt://u:p@broker.local:1883",
			area: "team",
			identity: identity("a", "proj-a", 101),
		});
		const b = await MqttHubClient.connect({
			url: "mqtt://u:p@broker.local:1883",
			area: "team",
			identity: identity("b", "proj-b", 202),
		});
		expect(a).not.toBeNull();
		expect(b).not.toBeNull();
		const fakeA = fakes[0]!;
		const fakeB = fakes[1]!;

		const got: ForumFrame[] = [];
		b!.onForum(frame => got.push(frame));

		await a!.forumPublish("triage", "hello");
		// Wire proof: the frame went out on the SHARED frames topic at QoS 1…
		const post = fakeA.published.find(entry => entry.topic === hubFramesTopic("team"));
		expect(post).toBeDefined();
		expect(post!.qos).toBe(1);
		const wire = JSON.parse((post!.payload as string).trim()) as ForumFrame;
		expect(wire.kind).toBe("forum");
		expect(wire.channel).toBe("triage");
		// …and receiving it (peer echo on that topic) fires handlers with no
		// hubTargetMatches involvement — targets play no part in forums.
		fakeB.deliver(hubFramesTopic("team"), post!.payload);
		await waitUntil(() => got.length === 1);
		expect(got[0]).toMatchObject({ channel: "triage", from: "a", body: "hello" });
		expect(typeof got[0]!.ts).toBe("number");

		a!.close();
		b!.close();
	}, 10_000);

	it("unsubscribing stops mqtt forum delivery", async () => {
		const a = await MqttHubClient.connect({
			url: "mqtt://u:p@broker.local:1883",
			area: "team",
			identity: identity("a", "proj-a", 101),
		});
		const b = await MqttHubClient.connect({
			url: "mqtt://u:p@broker.local:1883",
			area: "team",
			identity: identity("b", "proj-b", 202),
		});
		expect(a).not.toBeNull();
		expect(b).not.toBeNull();
		const fakeA = fakes[0]!;
		const fakeB = fakes[1]!;

		const got: ForumFrame[] = [];
		const unsubscribe = b!.onForum(frame => got.push(frame));

		await a!.forumPublish("triage", "one");
		fakeB.deliver(hubFramesTopic("team"), fakeA.published.find(entry => entry.topic === hubFramesTopic("team"))!.payload);
		await waitUntil(() => got.length === 1);
		unsubscribe();

		await a!.forumPublish("triage", "two");
		const second = [...fakeA.published.filter(entry => entry.topic === hubFramesTopic("team"))].pop()!;
		fakeB.deliver(hubFramesTopic("team"), second.payload);
		// deliver() runs every message handler synchronously — by the time it
		// returns, dispatch has already happened (or never will), so absence
		// here is deterministic without a timed wait.
		expect(got.length).toBe(1);
		expect(got[0]!.body).toBe("one");

		a!.close();
		b!.close();
	}, 10_000);

	it("invalid channel names throw before anything is published", async () => {
		const a = await MqttHubClient.connect({
			url: "mqtt://u:p@broker.local:1883",
			area: "team",
			identity: identity("a", "proj-a", 101),
		});
		expect(a).not.toBeNull();
		for (const channel of ["", "Bad_Name", "has space", "x".repeat(65)]) {
			await expect(a!.forumPublish(channel, "nope")).rejects.toThrow();
		}
		expect(fakes[0]!.published.filter(entry => entry.topic === hubFramesTopic("team"))).toHaveLength(0);
		a!.close();
	}, 10_000);
});
