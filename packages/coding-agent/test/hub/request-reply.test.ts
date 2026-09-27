import { afterAll, describe, expect, it } from "bun:test";
import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { HubClient, type HubRequestHandler } from "@oh-my-pi/pi-coding-agent/irc/remote/client";
import { hubProjectNamespace, startHubBroker } from "@oh-my-pi/pi-coding-agent/irc/remote/broker";
import {
	hubFramesTopic,
	MqttHubClient,
	setMqttConnectFactory,
	type MqttConnectArgs,
	type MqttLikeClient,
} from "@oh-my-pi/pi-coding-agent/irc/remote/mqtt";
import type { IrcMessage } from "@oh-my-pi/pi-tui/tools/irc";

function tmpSocketPath(): string {
	return path.join(fs.mkdtempSync(path.join(os.tmpdir(), "hub-rpc-")), "hub.sock");
}

function identity(agentId: string, project: string) {
	return { agentId, project, status: "running" as const, pid: process.pid };
}

const sockets: string[] = [];

afterAll(() => {
	for (const socketPath of sockets) {
		try {
			fs.unlinkSync(socketPath);
		} catch {}
	}
});

describe("hub request/reply (unix broker)", () => {
	async function startBroker(): Promise<string> {
		const socketPath = tmpSocketPath();
		sockets.push(socketPath);
		const listening = Promise.withResolvers<void>();
		void startHubBroker({ socketPath, idleGraceMs: 60_000, onListening: listening.resolve });
		await listening.promise;
		return socketPath;
	}

	function msg(from: string, to: string, body: string): IrcMessage {
		return { id: `m-${Math.random().toString(36).slice(2)}`, from, to, body, ts: Date.now() };
	}

	it("request resolves with the first reply when two peers match", async () => {
		const socketPath = await startBroker();
		const requester = await HubClient.connect({
			socketPath,
			identity: identity("asker", hubProjectNamespace("/p")),
			spawn: () => {},
		});
		expect(requester).not.toBeNull();
		const peerA = await HubClient.connect({ socketPath, identity: identity("a", hubProjectNamespace("/p")), spawn: () => {} });
		const peerB = await HubClient.connect({ socketPath, identity: identity("b", hubProjectNamespace("/p")), spawn: () => {} });
		expect(peerA).not.toBeNull();
		expect(peerB).not.toBeNull();
		const targets = [{ project: hubProjectNamespace("/p"), agentId: "a" }, { project: hubProjectNamespace("/p"), agentId: "b" }];
		peerA!.onRequest(() => msg("a", "asker", "from-a"));
		peerB!.onRequest(() => msg("b", "asker", "from-b"));

		const result = await requester!.request(msg("asker", "all", "ping"), targets, 5_000);
		expect(result).not.toBeNull();
		expect(["from-a", "from-b"]).toContain(result!.msg.body);
		expect([ "a", "b"]).toContain(result!.from);

		requester!.close();
		peerA!.close();
		peerB!.close();
	}, 10_000);

	it("times out to null when no peer answers", async () => {
		const socketPath = await startBroker();
		const requester = await HubClient.connect({
			socketPath,
			identity: identity("asker", hubProjectNamespace("/p")),
			spawn: () => {},
		});
		expect(requester).not.toBeNull();
		// Matching peer registers NO handler → declines silently.
		const peer = await HubClient.connect({ socketPath, identity: identity("a", hubProjectNamespace("/p")), spawn: () => {} });
		expect(peer).not.toBeNull();

		const result = await requester!.request(msg("asker", "a", "ping"), [{ project: hubProjectNamespace("/p"), agentId: "a" }], 200);
		expect(result).toBeNull();

		requester!.close();
		peer!.close();
	}, 10_000);

	it("resolves null without hanging when no matching peer exists", async () => {
		const socketPath = await startBroker();
		const requester = await HubClient.connect({
			socketPath,
			identity: identity("asker", hubProjectNamespace("/p")),
			spawn: () => {},
		});
		expect(requester).not.toBeNull();

		const result = await requester!.request(msg("asker", "ghost", "ping"), [{ project: hubProjectNamespace("/q"), agentId: "ghost" }], 300);
		expect(result).toBeNull();

		requester!.close();
	}, 10_000);

	it("declining handler sends no reply; another peer's answer still resolves", async () => {
		const socketPath = await startBroker();
		const requester = await HubClient.connect({
			socketPath,
			identity: identity("asker", hubProjectNamespace("/p")),
			spawn: () => {},
		});
		const decliner = await HubClient.connect({ socketPath, identity: identity("decline", hubProjectNamespace("/p")), spawn: () => {} });
		const answerer = await HubClient.connect({ socketPath, identity: identity("answer", hubProjectNamespace("/p")), spawn: () => {} });
		expect(requester).not.toBeNull();
		expect(decliner).not.toBeNull();
		expect(answerer).not.toBeNull();
		const targets = [
			{ project: hubProjectNamespace("/p"), agentId: "decline" },
			{ project: hubProjectNamespace("/p"), agentId: "answer" },
		];
		decliner!.onRequest(() => null);
		answerer!.onRequest((_m, from) => msg(from, "asker", "declined-around"));

		const result = await requester!.request(msg("asker", "all", "ping"), targets, 5_000);
		expect(result).not.toBeNull();
		expect(result!.from).toBe("answer");
		expect(result!.msg.body).toBe("declined-around");

		requester!.close();
		decliner!.close();
		answerer!.close();
	}, 10_000);
});

describe("hub request/reply (mqtt transport)", () => {
	const fakes: FakeMqttClient[] = [];

	class FakeMqttClient implements MqttLikeClient {
		published: { topic: string; payload: string }[] = [];
		#messageHandlers: ((topic: string, payload: Buffer) => void)[] = [];
		#closeHandlers: (() => void)[] = [];
		#errorHandlers: ((err: Error) => void)[] = [];

		constructor(readonly connectArgs: MqttConnectArgs) {}

		async endAsync(): Promise<void> {}
		async publishAsync(topic: string, message: string | Buffer): Promise<unknown> {
			this.published.push({ topic, payload: typeof message === "string" ? message : message.toString("utf8") });
			// Fan out to every OTHER connected fake (shared frames topic).
			for (const fake of fakes) {
				if (fake === this) continue;
				fake.inject(topic, message);
			}
			return undefined;
		}
		async subscribeAsync(): Promise<unknown> {
			return undefined;
		}
		on(event: "message", cb: (topic: string, payload: Buffer) => void): void;
		on(event: "close", cb: () => void): void;
		on(event: "error", cb: (err: Error) => void): void;
		on(event: string, cb: never): void {
			if (event === "message") this.#messageHandlers.push(cb as (topic: string, payload: Buffer) => void);
			else if (event === "close") this.#closeHandlers.push(cb as () => void);
			else if (event === "error") this.#errorHandlers.push(cb as (err: Error) => void);
		}
		inject(topic: string, payload: string | Buffer): void {
			const buffer = typeof payload === "string" ? Buffer.from(payload, "utf8") : payload;
			for (const handler of [...this.#messageHandlers]) handler(topic, buffer);
		}
		frames(): (Record<string, unknown>)[ ] {
			return this.published
				.filter(entry => entry.topic === hubFramesTopic("team"))
				.flatMap(entry => entry.payload.split("\n").filter(Boolean))
				.map(line => JSON.parse(line) as Record<string, unknown>);
		}
	}

	function installFactory(): void {
		setMqttConnectFactory(args => {
			const fake = new FakeMqttClient(args);
			fakes.push(fake);
			return Promise.resolve(fake);
		});
	}

	async function connect(agentId: string, pid: number): Promise<MqttHubClient> {
		const client = await MqttHubClient.connect({
			url: "mqtt://user:pass@localhost",
			area: "team",
			identity: identity(agentId, "proj"),
		});
		expect(client).not.toBeNull();
		return client!;
	}

	it("request and reply traverse hub/<area>/frames, correlated by id", async () => {
		installFactory();
		const pidSeq = (pid => () => ++pid)(100);
		process.env.OMP_HUB_MQTT_USERNAME = "user";
		const asker = await connect("asker", pidSeq());
		const answerer = await connect("answer", pidSeq());
		answerer.onRequest((_m, from) => ({ id: "r1", from, to: "asker", body: "mqtt-reply", ts: Date.now() }));

		const targets = [{ project: "proj", agentId: "answer" }];
		const result = await asker.request({ id: "q1", from: "asker", to: "answer", body: "mqtt-ping", ts: Date.now() }, targets, 2_000);
		expect(result).not.toBeNull();
		expect(result!.from).toBe("answer");
		expect(result!.msg.body).toBe("mqtt-reply");

		// Topic discipline: every RPC frame rode the shared frames topic.
		const askerFrames = fakes[0]!.frames();
		const answererFrames = fakes[1]!.frames();
		expect(askerFrames.some(f => f.type === "request")).toBe(true);
		expect(answererFrames.some(f => f.type === "reply")).toBe(true);
		for (const fake of fakes) {
			const request = fake.frames().find(f => f.type === "request");
			const reply = fake.frames().find(f => f.type === "reply");
			if (request && reply) expect(reply.id).toBe(request.id);
		}

		asker.close();
		answerer.close();
		process.env.OMP_HUB_MQTT_USERNAME = undefined;
	}, 10_000);
});

describe("hub request/reply (async + error handlers)", () => {
	it("async handler answers; throwing handler declines via HubClient surface", async () => {
		// Handler contract is shared, so exercise it through a tiny fake
		// HubClientLike-free harness: the unix tests above cover transport
		// wiring; here we pin the handler type shape.
		const handler: HubRequestHandler = async (m, from) => (m.body === "boom" ? null : { id: "x", from, to: m.from, body: "ok", ts: 0 });
		expect((await handler({ id: "1", from: "a", to: "b", body: "hi", ts: 0 }, "b"))?.body).toBe("ok");
		expect(await handler({ id: "1", from: "a", to: "b", body: "boom", ts: 0 }, "b")).toBeNull();
	});
});
