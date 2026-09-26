/**
 * Hub client: connects the current omp process to the system-scope broker.
 * Used by IrcBus/AgentRegistry when `hub.systemScope.enabled` is on; every
 * failure path (disabled, unreachable, timeout) resolves to `null` so callers
 * fall back silently to today's in-process-only behavior. Never hangs: every
 * request is bounded by a short timeout.
 */
import * as net from "node:net";
import * as fs from "node:fs";
import type { IrcMessage } from "@oh-my-pi/pi-tui/tools/irc";
import {
	DEFAULT_HUB_IDLE_GRACE_MS,
	encodeFrame,
	FrameStream,
	HUB_IDLE_GRACE_ENV,
	HUB_SOCKET_PATH_ENV,
	type HubClientFrame,
	type HubRosterEntry,
	type HubServerFrame,
	type HubTarget,
} from "./protocol";

/** How long any single client operation may take before failing open. */
const REQUEST_TIMEOUT_MS = 2_000;

export interface HubAgentIdentity {
	agentId: string;
	project: string;
	status: "running" | "idle";
	pid: number;
	sessionFile?: string;
}

export interface HubClientOptions {
	socketPath: string;
	identity: HubAgentIdentity;
	/** Test seam: overrides spawn behavior (tests start the broker in-process). */
	spawn?: (socketPath: string) => void;
	/** Test seam: suppresses the daemon's own idle exit. */
	idleGraceMs?: number;
}

/** Delivered cross-process message plus the broker's publish outcome. */
export interface HubPublishResult {
	results: { to: string; ok: boolean; error?: string }[];
}

type Waiter = { resolve: (frame: HubServerFrame) => void };

/**
 * One process's connection to the hub broker. Spawn-on-connect: when the
 * socket is unreachable the client spawns the broker worker and retries,
 * mirroring the launch broker's race-tolerant handoff.
 */
export class HubClient {
	#socket: net.Socket | undefined;
	#frames = new FrameStream();
	#pending = new Map<string, Waiter[]>();
	#deliveries: ((msg: IrcMessage) => void) | undefined;
	#identity: HubAgentIdentity | undefined;
	#closed = false;
	#onClose: (() => void) | undefined;

	private constructor(
		readonly socketPath: string,
		private readonly spawnBroker: ((socketPath: string) => void) | undefined,
		private readonly idleGraceMs: number | undefined,
	) {}

	/**
	 * Connect (spawning the broker on first use) and register `identity`.
	 * Resolves null when the hub stays unreachable within the timeout.
	 */
	static async connect(options: HubClientOptions): Promise<HubClient | null> {
		const client = new HubClient(options.socketPath, options.spawn ?? spawnHubBrokerDefault, options.idleGraceMs);
		const connected = await client.#connectWithSpawn();
		if (!connected) return null;
		const welcome = await client.#request({ type: "hello", agents: [options.identity] });
		if (welcome?.type !== "welcome") {
			client.close();
			return null;
		}
		client.#identity = options.identity;
		return client;
	}

	/** Latest broker roster (other connections' agents). */
	async roster(): Promise<HubRosterEntry[]> {
		const frame = await this.#request({ type: "roster" });
		return frame?.type === "roster" ? frame.roster : [];
	}

	/**
	 * Publish a message to specific recipients (`to:"all"` callers expand to
	 * `targets` themselves, scoped to the project namespace).
	 */
	async publish(msg: IrcMessage, targets: HubTarget[]): Promise<HubPublishResult | null> {
		const frame = await this.#request({ type: "publish", msg, targets });
		return frame?.type === "publishAck" ? { results: frame.results } : null;
	}

	/** Update this process's roster status (running/idle transitions). */
	async setStatus(status: "running" | "idle"): Promise<void> {
		if (!this.#socket) return;
		if (!this.#identity) return;
		this.#socket.write(encodeFrame({ type: "status", agentId: this.#identity.agentId, status }) as string);
	}

	/** Register the local sink for broker-relayed deliveries. */
	onDelivery(sink: (msg: IrcMessage) => void): void {
		this.#deliveries = sink;
	}

	/** Invoked when the socket closes (broker death); hub-manager clears its cached client. */
	onClose(handler: () => void): void {
		this.#onClose = handler;
	}

	/** Send bye and detach; safe to call repeatedly. */
	close(): void {
		if (this.#closed) return;
		this.#closed = true;
		const socket = this.#socket;
		this.#socket = undefined;
		if (socket && !socket.destroyed) {
			try {
				socket.write(encodeFrame({ type: "bye" }) as string);
			} catch {
				// best-effort farewell
			}
			socket.end();
		}
	}

	async #connectWithSpawn(): Promise<boolean> {
		const deadline = Date.now() + REQUEST_TIMEOUT_MS;
		for (let attempt = 0; ; attempt++) {
			if (await this.#bindSocket()) return true;
			if (attempt === 0 && this.spawnBroker) this.spawnBroker(this.socketPath);
			if (Date.now() >= deadline) return false;
			await Bun.sleep(50);
		}
	}

	async #bindSocket(): Promise<boolean> {
		// No socket file: guaranteed ENOENT; skip the connect dance entirely.
		if (!fs.existsSync(this.socketPath)) return false;
		const { promise, resolve } = Promise.withResolvers<boolean>();
		const socket = net.connect(this.socketPath);
		socket.setTimeout(REQUEST_TIMEOUT_MS);
		const finish = (ok: boolean) => {
			socket.setTimeout(0);
			socket.removeListener("connect", onConnect);
			socket.removeListener("error", onError);
			if (ok) resolve(true);
			else {
				socket.destroy();
				resolve(false);
			}
		};
		const onConnect = () => finish(true);
		const onError = () => finish(false);
		socket.once("connect", onConnect);
		socket.once("error", onError);
		socket.once("timeout", () => finish(false));
		if (!(await promise)) return false;

		this.#socket = socket;
		socket.setEncoding("utf8");
		socket.on("data", chunk => {
			for (const frame of this.#frames.push(chunk)) this.#onFrame(frame as HubServerFrame);
		});
		socket.on("close", () => {
			if (this.#socket === socket) this.#socket = undefined;
			this.#rejectPending(new Error("hub broker connection closed"));
			this.#onClose?.();
		});
		socket.on("error", () => socket.destroy());
		return true;
	}

	#onFrame(frame: HubServerFrame): void {
		switch (frame.type) {
			case "deliver": {
				this.#deliveries?.(frame.msg);
				break;
			}
			default: {
				const waiters = this.#pending.get(frame.type);
				const waiter = waiters?.shift();
				if (waiters && waiters.length === 0) this.#pending.delete(frame.type);
				waiter?.resolve(frame);
			}
		}
	}

	/**
	 * Send a request and await the matching response frame. Keyed by response
	 * type: hello→welcome, roster→roster, publish→publishAck, ping→pong.
	 */
	static readonly #RESPONSE_TYPE: Partial<Record<HubClientFrame["type"], HubServerFrame["type"]>> = {
		hello: "welcome",
		roster: "roster",
		publish: "publishAck",
		ping: "pong",
	};

	#request(frame: HubClientFrame): Promise<HubServerFrame | null> {
		const socket = this.#socket;
		if (!socket || this.#closed) return Promise.resolve(null);
		const responseKey = HubClient.#RESPONSE_TYPE[frame.type];
		if (!responseKey) return Promise.resolve(null);
		const { promise, resolve } = Promise.withResolvers<HubServerFrame | null>();
		const timer = setTimeout(() => {
			const waiters = this.#pending.get(responseKey);
			if (waiters) {
				const index = waiters.findIndex(waiter => waiter.resolve === resolveWaiter);
				if (index !== -1) waiters.splice(index, 1);
				if (waiters.length === 0) this.#pending.delete(responseKey);
			}
			resolve(null);
		}, REQUEST_TIMEOUT_MS);
		timer.unref?.();
		const resolveWaiter = (response: HubServerFrame) => {
			clearTimeout(timer);
			resolve(response);
		};
		const waiters = this.#pending.get(responseKey) ?? [];
		waiters.push({ resolve: resolveWaiter });
		this.#pending.set(responseKey, waiters);
		socket.write(encodeFrame(frame) as string);
		return promise;
	}

	#rejectPending(error: Error): void {
		for (const [type, waiters] of this.#pending) {
			for (const waiter of waiters) waiter.resolve({ type: "error", message: error.message });
			this.#pending.delete(type);
		}
	}
}

/** Default spawn: launch the detached hub-broker worker for `socketPath`. */
function spawnHubBrokerDefault(socketPath: string): void {
	const { resolveWorkerSpawnCmd, workerEnvFromParent } =
		require("../../subprocess/worker-client") as typeof import("../../subprocess/worker-client");
	const { HUB_BROKER_WORKER_ARG } = require("../../cli/worker-selectors") as {
		HUB_BROKER_WORKER_ARG: string;
	};
	const spawn = resolveWorkerSpawnCmd(HUB_BROKER_WORKER_ARG);
	const overlay: Record<string, string> = {
		[HUB_SOCKET_PATH_ENV]: socketPath,
		[HUB_IDLE_GRACE_ENV]: String(DEFAULT_HUB_IDLE_GRACE_MS),
	};
	const child = Bun.spawn(spawn.cmd, {
		cwd: spawn.cwd,
		env: workerEnvFromParent(overlay),
		stdin: "ignore",
		stdout: "ignore",
		stderr: "ignore",
		detached: true,
	});
	child.unref();
}
