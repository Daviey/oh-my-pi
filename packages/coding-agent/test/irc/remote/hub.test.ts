import { afterAll, describe, expect, it } from "bun:test";
import * as fs from "node:fs";
import * as net from "node:net";
import * as os from "node:os";
import * as path from "node:path";
import { HubClient } from "@oh-my-pi/pi-coding-agent/irc/remote/client";
import { hubProjectNamespace, parentDirIsUnsafe, startHubBroker } from "@oh-my-pi/pi-coding-agent/irc/remote/broker";
import { encodeFrame, FrameStream, type HubServerFrame } from "@oh-my-pi/pi-coding-agent/irc/remote/protocol";

function tmpSocketPath(): string {
	return path.join(fs.mkdtempSync(path.join(os.tmpdir(), "hub-test-")), "hub.sock");
}

function identity(agentId: string, project: string) {
	return { agentId, project, status: "running" as const, pid: process.pid };
}

/** Raw socket client for protocol-level assertions. */
class RawPeer {
	#socket: net.Socket;
	#frames = new FrameStream();
	received: HubServerFrame[] = [];
	#notify: (() => void) | undefined;
	closed = Promise.withResolvers<void>();

	constructor(socketPath: string) {
		this.#socket = net.connect(socketPath);
		this.#socket.setEncoding("utf8");
		this.#socket.on("data", chunk => {
			for (const frame of this.#frames.push(chunk)) {
				this.received.push(frame as HubServerFrame);
				this.#wake();
			}
		});
		this.#socket.on("close", () => this.closed.resolve());
	}

	async connect(): Promise<void> {
		const { promise, resolve, reject } = Promise.withResolvers<void>();
		this.#socket.once("connect", resolve);
		this.#socket.once("error", reject);
		// A refused connection is destroyed by the broker; settle either way.
		this.#socket.once("close", resolve);
		await promise;
	}

	write(frame: object): void {
		this.#socket.write(encodeFrame(frame as never) as string);
	}

	next(timeoutMs = 2_000): Promise<HubServerFrame> {
		const buffered = this.received.shift();
		if (buffered) return Promise.resolve(buffered);
		const { promise, resolve } = Promise.withResolvers<HubServerFrame>();
		const timer = setTimeout(() => {
			this.#notify = undefined;
			resolve({ type: "error", message: "timeout" });
		}, timeoutMs);
		this.#notify = () => {
			const frame = this.received.shift();
			if (!frame) return;
			clearTimeout(timer);
			this.#notify = undefined;
			resolve(frame);
		};
		return promise;
	}

	#wake(): void {
		this.#notify?.();
	}

	end(): void {
		this.#socket.end();
	}
}

const sockets: string[] = [];

describe("hub broker protocol", () => {
	function makeBrokerPath(): string {
		const socketPath = tmpSocketPath();
		sockets.push(socketPath);
		return socketPath;
	}

	it("registers roster, answers roster requests, and relays publish to a peer", async () => {
		const socketPath = makeBrokerPath();
		const listening = Promise.withResolvers<void>();
		const brokerDone = startHubBroker({ socketPath, idleGraceMs: 60_000, onListening: listening.resolve });
		await listening.promise;

		const alice = new RawPeer(socketPath);
		await alice.connect();
		alice.write({ type: "hello", agents: [identity("alice", "p1")] });
		const welcome = await alice.next();
		expect(welcome.type).toBe("welcome");
		expect(welcome.type === "welcome" && welcome.roster).toEqual([]);

		const bob = new RawPeer(socketPath);
		await bob.connect();
		bob.write({ type: "hello", agents: [identity("bob", "p2")] });
		const bobWelcome = (await bob.next()) as Extract<HubServerFrame, { type: "welcome" }>;
		expect(bobWelcome.roster.map(entry => entry.agentId)).toEqual(["alice"]);

		// Bob requests the roster; sees Alice (not himself).
		bob.write({ type: "roster" });
		const roster = (await bob.next()) as Extract<HubServerFrame, { type: "roster" }>;
		expect(roster.roster.map(entry => entry.agentId)).toEqual(["alice"]);

		// Alice publishes to Bob cross-project.
		alice.write({
			type: "publish",
			msg: { id: "m1", from: "alice", to: "bob", body: "hi", ts: 1 },
			targets: [{ project: "p2", agentId: "bob" }],
		});
		const ack = (await alice.next()) as Extract<HubServerFrame, { type: "publishAck" }>;
		expect(ack.results).toEqual([{ to: "bob", ok: true }]);
		const delivered = (await bob.next()) as Extract<HubServerFrame, { type: "deliver" }>;
		expect(delivered.msg.body).toBe("hi");

		// Project mismatch is rejected.
		alice.write({
			type: "publish",
			msg: { id: "m2", from: "alice", to: "bob", body: "nope", ts: 2 },
			targets: [{ project: "wrong", agentId: "bob" }],
		});
		const failed = (await alice.next()) as Extract<HubServerFrame, { type: "publishAck" }>;
		expect(failed.results[0]).toMatchObject({ ok: false, error: "project-mismatch" });

		alice.end();
		bob.end();
		void brokerDone;
	}, 10_000);

	it("ping/pong works", async () => {
		const socketPath = makeBrokerPath();
		const listening = Promise.withResolvers<void>();
		void startHubBroker({ socketPath, idleGraceMs: 60_000, onListening: listening.resolve });
		await listening.promise;
		const peer = new RawPeer(socketPath);
		await peer.connect();
		peer.write({ type: "ping" });
		const pong = await peer.next();
		expect(pong.type).toBe("pong");
		peer.end();
	}, 10_000);

	it("refuses cross-uid connections", async () => {
		const socketPath = makeBrokerPath();
		const listening = Promise.withResolvers<void>();
		// Deterministic refusal: the peerUid seam reports a foreign uid for every
		// connection, so the broker must destroy each socket before hello.
		void startHubBroker({
			socketPath,
			idleGraceMs: 60_000,
			onListening: listening.resolve,
			peerUid: () => (process.getuid?.() ?? 0) + 1,
		});
		await listening.promise;

		const peer = new RawPeer(socketPath);
		await peer.connect();
		peer.write({ type: "hello", agents: [identity("mallory", "p")] });
		// The broker destroys the socket before any welcome: the next frame
		// never arrives, so next() settles on the timeout sentinel.
		const response = await peer.next(1_500);
		expect(response.type).toBe("error");
		expect((response as { message?: string }).message).toBe("timeout");

		// Control: with the real resolver (same uid) the same handshake gets a welcome.
		const controlPath = makeBrokerPath();
		const controlListening = Promise.withResolvers<void>();
		void startHubBroker({ socketPath: controlPath, idleGraceMs: 60_000, onListening: controlListening.resolve });
		await controlListening.promise;
		const good = new RawPeer(controlPath);
		await good.connect();
		good.write({ type: "hello", agents: [identity("good", "p")] });
		expect((await good.next()).type).toBe("welcome");
		good.end();
	}, 10_000);

	it("socket is created 0600", async () => {
		const socketPath = makeBrokerPath();
		const listening = Promise.withResolvers<void>();
		void startHubBroker({ socketPath, idleGraceMs: 60_000, onListening: listening.resolve });
		await listening.promise;
		const mode = fs.statSync(socketPath).mode & 0o777;
		expect(mode).toBe(0o600);
	}, 10_000);

	it("exits after the idle grace with no peers", async () => {
		const socketPath = makeBrokerPath();
		// Real-timer test: exercises the broker's own idle-grace timer against
		// the platform clock; deterministic fake timers cannot advance the
		// broker's independent Node timer from the test process.
		const listening = Promise.withResolvers<void>();
		void startHubBroker({ socketPath, idleGraceMs: 150, onListening: listening.resolve });
		await listening.promise;
		expect(fs.existsSync(socketPath)).toBe(true);
		const { promise, resolve } = Promise.withResolvers<void>();
		const poll = setInterval(() => {
			if (!fs.existsSync(socketPath)) {
				clearInterval(poll);
				resolve();
			}
		}, 25);
		setTimeout(() => resolve(), 3_000);
		await promise;
	}, 10_000);
});

describe("hub client", () => {
	it("two clients in different project namespaces exchange a message", async () => {
		const socketPath = tmpSocketPath();
		const listening = Promise.withResolvers<void>();
		void startHubBroker({ socketPath, idleGraceMs: 60_000, onListening: listening.resolve });
		await listening.promise;

		const delivered: unknown[] = [];
		const alice = await HubClient.connect({
			socketPath,
			identity: identity("alice", hubProjectNamespace("/proj-a")),
			spawn: () => {},
		});
		expect(alice).not.toBeNull();

		const bob = await HubClient.connect({
			socketPath,
			identity: identity("bob", hubProjectNamespace("/proj-b")),
			spawn: () => {},
		});
		expect(bob).not.toBeNull();
		bob!.onDelivery(msg => delivered.push(msg));

		const roster = await alice!.roster();
		expect(roster.map(entry => entry.agentId)).toEqual(["bob"]);

		const result = await alice!.publish({ id: "x1", from: "alice", to: "bob", body: "cross-project hello", ts: 1 }, [
			{ project: hubProjectNamespace("/proj-b"), agentId: "bob" },
		]);
		expect(result?.results).toEqual([{ to: "bob", ok: true }]);
		for (let i = 0; i < 100 && delivered.length === 0; i++) await Bun.sleep(10);
		expect(delivered).toHaveLength(1);
		expect((delivered[0] as { body: string }).body).toBe("cross-project hello");

		// Wrong-namespace publish fails closed.
		const mismatch = await alice!.publish({ id: "x2", from: "alice", to: "bob", body: "nope", ts: 2 }, [
			{ project: hubProjectNamespace("/proj-a"), agentId: "bob" },
		]);
		expect(mismatch?.results[0]).toMatchObject({ ok: false, error: "project-mismatch" });

		alice!.close();
		bob!.close();
	}, 10_000);

	it("returns null when the broker is unreachable", async () => {
		const socketPath = path.join(fs.mkdtempSync(path.join(os.tmpdir(), "hub-dead-")), "hub.sock");
		const client = await HubClient.connect({ socketPath, identity: identity("a", "p"), spawn: () => {} });
		expect(client).toBeNull();
	}, 10_000);
});

describe("hub socket path policy", () => {
	it("flags group/world-writable parents as unsafe", () => {
		const dir = fs.mkdtempSync(path.join(os.tmpdir(), "hub-perm-"));
		fs.chmodSync(dir, 0o777);
		expect(parentDirIsUnsafe(dir)).toBe(true);
		fs.chmodSync(dir, 0o700);
		expect(parentDirIsUnsafe(dir)).toBe(false);
		fs.rmSync(dir, { recursive: true });
	});

	it("project namespaces are stable per path and distinct across paths", () => {
		expect(hubProjectNamespace("/a/b")).toBe(hubProjectNamespace("/a/b"));
		expect(hubProjectNamespace("/a/b")).not.toBe(hubProjectNamespace("/a/c"));
	});
});

afterAll(() => {
	for (const socketPath of sockets) {
		try {
			fs.unlinkSync(socketPath);
		} catch {}
	}
});
