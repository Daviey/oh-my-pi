/**
 * MQTT transport for the system-scope hub: a PEER of the unix-socket broker,
 * not a bridge through it. Same frames (NDJSON via encodeFrame/FrameStream),
 * same roster semantics — different wire: every topic lives under a NAMED
 * AREA (`hub/<area>/...`), so peers on a shared broker only see each other
 * when they opt into the same area.
 *
 * Auth is mandatory: OMP_HUB_MQTT_USERNAME/OMP_HUB_MQTT_PASSWORD env vars or
 * userinfo in the URL (mqtt://user:pass@host). Missing both → fail closed
 * (null) — never an anonymous session on a shared broker.
 *
 * Topic map (QoS 1 throughout, clean start):
 * - `hub/<area>/frames`              every frame. A `publish` from one peer
 *                                    is matched against each receiver's own
 *                                    identity (same matcher as the unix
 *                                    broker), delivered locally when it
 *                                    matches, and acknowledged with a
 *                                    `publishAck` correlated by frame id.
 *                                    Only matching peers ack — a target
 *                                    nobody matches produces no row and the
 *                                    sender's request times out (null),
 *                                    mirroring the unix miss path.
 * - `hub/<area>/presence/<agentId>`  RETAINED HubRosterEntry snapshot; an
 *                                    empty payload clears; the LWT publishes
 *                                    that same empty payload, so an unclean
 *                                    death still clears the roster promptly.
 *                                    Same-id peers share the agentId-keyed
 *                                    topic (agent ids repeat across
 *                                    processes): last writer's snapshot is
 *                                    what new subscribers retain-see; live
 *                                    presence publishes still reach everyone.
 */
import { logger } from "@oh-my-pi/pi-utils";
import type { IrcMessage } from "@oh-my-pi/pi-tui/tools/irc";
import type * as MqttModule from "mqtt";
import { resolveHubArea } from "../../hub/settings";
import {
	encodeFrame,
	FrameStream,
	hubTargetMatches,
	type HubClientFrame,
	type HubRosterEntry,
	type HubServerFrame,
	type HubTarget,
} from "./protocol";
import type { HubAgentIdentity, HubClientLike, HubPublishResult } from "./client";

/** Env vars carrying broker credentials. Secrets never live in config values. */
export const OMP_HUB_MQTT_USERNAME_ENV = "OMP_HUB_MQTT_USERNAME";
export const OMP_HUB_MQTT_PASSWORD_ENV = "OMP_HUB_MQTT_PASSWORD";

/** Frames topic for an area — every peer publishes and subscribes here. */
export function hubFramesTopic(area: string): string {
	return `hub/${area}/frames`;
}

/** Presence topic for one agent under an area. */
export function hubPresenceTopic(area: string, agentId: string): string {
	return `hub/${area}/presence/${agentId}`;
}

/** How long any single client operation may take before failing open. */
const REQUEST_TIMEOUT_MS = 2_000;

/** Minimal MqttClient surface this transport needs — keeps the fake lean. */
export interface MqttLikeClient {
	endAsync(force?: boolean): Promise<void>;
	publishAsync(topic: string, message: string | Buffer, opts?: { qos?: number; retain?: boolean }): Promise<unknown>;
	subscribeAsync(topic: string | string[], opts?: { qos?: number }): Promise<unknown>;
	on(event: "message", cb: (topic: string, payload: Buffer) => void): void;
	on(event: "close", cb: () => void): void;
	on(event: "error", cb: (err: Error) => void): void;
}

/** What the connect factory returns; `null` = broker refused/unreachable. */
export type MqttConnectResult = MqttLikeClient | null;

/** Arguments the connect factory receives. */
export interface MqttConnectArgs {
	/** Broker URL (userinfo already resolved into username/password). */
	url: string;
	/** Unique per-process client id: omp-<area>-<agentId>-<pid>. */
	clientId: string;
	username: string;
	password?: string;
	/** LWT topic: an empty retained payload clears this presence on death. */
	willTopic: string;
}

/**
 * Overridable connect factory (test seam): tests inject a fake via
 * {@link setMqttConnectFactory}; production lazily requires mqtt so the
 * dependency never loads for unix-only processes.
 */
export let mqttConnectFactory: (args: MqttConnectArgs) => Promise<MqttConnectResult> = mqttConnectDefault;

/** Replace the connect factory; null restores the default. */
export function setMqttConnectFactory(
	factory: ((args: MqttConnectArgs) => Promise<MqttConnectResult>) | null,
): void {
	mqttConnectFactory = factory ?? mqttConnectDefault;
}

/** Production connect: mqtt.js, no auto-reconnect (the retry ladder owns that). */
function mqttConnectDefault(args: MqttConnectArgs): Promise<MqttConnectResult> {
	const lib = require("mqtt") as typeof MqttModule;
	const client = lib.connect(args.url, {
		clientId: args.clientId,
		username: args.username,
		password: args.password,
		clean: true,
		protocolVersion: 5,
		reconnectPeriod: 0,
		connectTimeout: REQUEST_TIMEOUT_MS,
		will: {
			topic: args.willTopic,
			payload: Buffer.alloc(0),
			qos: 1,
			retain: true,
		},
	});
	const { promise, resolve } = Promise.withResolvers<MqttConnectResult>();
	let settled = false;
	const settle = (result: MqttConnectResult) => {
		if (settled) return;
		settled = true;
		clearTimeout(timer);
		resolve(result);
	};
	const timer = setTimeout(() => {
		client.end(true);
		settle(null);
	}, REQUEST_TIMEOUT_MS + 500);
	timer.unref?.();
	client.removeAllListeners("connect");
	client.once("connect", () => settle(client as unknown as MqttLikeClient));
	// A refusal can surface as error or a bare close; either fails closed.
	client.removeAllListeners("error");
	client.once("error", () => {
		client.end(true);
		settle(null);
	});
	client.removeAllListeners("close");
	client.once("close", () => settle(null));
	return promise;
}

/** Per-publish waiter: merges ack rows until every target agentId answered. */
interface AckWaiter {
	/** Distinct agentIds still awaiting a first ack row. */
	pending: Set<string>;
	rows: Map<string, { to: string; ok: boolean; error?: string }>;
	resolve: (result: HubPublishResult | null) => void;
}

/** One process's MQTT presence in a named hub area. */
export class MqttHubClient implements HubClientLike {
	#client: MqttLikeClient | null = null;
	#frames = new FrameStream();
	#acks = new Map<string, AckWaiter>();
	/** Message ids this process published (self-echo suppression). */
	#selfPublishes = new Set<string>();
	#deliveries: ((msg: IrcMessage) => void) | undefined;
	#identity: HubAgentIdentity | undefined;
	#closed = false;
	#onClose: (() => void) | undefined;
	#roster = new Map<string, HubRosterEntry>();
	readonly area: string;
	readonly url: string;

	private constructor(area: string, url: string) {
		this.area = area;
		this.url = url;
	}

	/**
	 * Connect to the area's topics and announce this process. Resolves null
	 * (fail closed) when credentials are absent or the broker refuses.
	 */
	static async connect(options: { url: string; area: string; identity: HubAgentIdentity }): Promise<MqttHubClient | null> {
		const area = resolveHubArea(options.area);
		const url = options.url.trim();
		const { username, password } = resolveMqttCredentials(url);
		if (!username) {
			logger.warn(
				`hub mqtt: missing broker credentials; refusing anonymous connect (set ${OMP_HUB_MQTT_USERNAME_ENV}/${OMP_HUB_MQTT_PASSWORD_ENV} or use mqtt://user:pass@host)`,
			);
			return null;
		}
		const identity = { ...options.identity };
		const client = new MqttHubClient(area, url);
		return (await client.#start(username, password, identity)) ? client : null;
	}

	/** Wire the live client: subscriptions, presence, frame routing. */
	async #start(username: string, password: string | undefined, identity: HubAgentIdentity): Promise<boolean> {
		const presenceTopic = hubPresenceTopic(this.area, identity.agentId);
		let connected: MqttConnectResult;
		try {
			connected = await mqttConnectFactory({
				url: this.url,
				clientId: `omp-${this.area}-${identity.agentId}-${identity.pid}`,
				username,
				password,
				willTopic: presenceTopic,
			});
		} catch (error) {
			logger.warn("hub mqtt: connect threw", { error: errorMessage(error) });
			return false;
		}
		if (!connected) return false;
		this.#client = connected;
		this.#identity = identity;
		connected.on("message", (topic, payload) => this.#onMessage(topic, payload));
		connected.on("close", () => {
			this.#flushPendingAcks();
			this.#onClose?.();
		});
		connected.on("error", () => {
			// errors surface via close; nothing to do here
		});
		try {
			await connected.subscribeAsync(hubFramesTopic(this.area), { qos: 1 });
			// Retained snapshots for every present peer ride on this subscription.
			await connected.subscribeAsync(`hub/${this.area}/presence/+`, { qos: 1 });
			await connected.publishAsync(presenceTopic, JSON.stringify(identity), { qos: 1, retain: true });
		} catch (error) {
			logger.warn("hub mqtt: setup failed", { error: errorMessage(error) });
			await this.#endClient(connected);
			this.#client = null;
			this.#identity = undefined;
			return false;
		}
		return true;
	}

	/** Latest roster: retained presence snapshots merged with live updates.
	 *  Own registration is excluded at merge time (same agentId AND pid),
	 *  exactly like the unix broker's per-connection snapshot. */
	async roster(): Promise<HubRosterEntry[]> {
		return [...this.#roster.values()];
	}

	/**
	 * Publish a message to specific recipients. The frame fans out on the
	 * shared frames topic; matching peers deliver locally and ack, and this
	 * resolves once every target agentId has an ack row — or on timeout with
	 * the partial rows (null when nobody answered at all), mirroring the
	 * unix client's miss path.
	 */
	async publish(msg: IrcMessage, targets: HubTarget[]): Promise<HubPublishResult | null> {
		const client = this.#client;
		if (!client || this.#closed) return null;
		const ids = [...new Set(targets.filter(target => target?.agentId).map(target => target.agentId))];
		if (ids.length === 0) return { results: [] };
		const id = typeof msg?.id === "string" ? msg.id : "";
		const { promise, resolve } = Promise.withResolvers<HubPublishResult | null>();
		const waiter: AckWaiter = { pending: new Set(ids), rows: new Map(), resolve };
		const timer = setTimeout(() => {
			this.#acks.delete(id);
			this.#selfPublishes.delete(id);
			resolve(waiter.rows.size ? { results: [...waiter.rows.values()] } : null);
		}, REQUEST_TIMEOUT_MS);
		timer.unref?.();
		this.#acks.set(id, waiter);
		this.#selfPublishes.add(id);
		void client
			.publishAsync(hubFramesTopic(this.area), encodeFrame({ type: "publish", msg, targets }), { qos: 1 })
			.catch(() => {
				// leave the timer to fail the request
			});
		return promise;
	}

	/** Refresh retained presence with the new status/activity gist. */
	async setStatus(status: "running" | "idle", activity?: string): Promise<void> {
		const identity = this.#identity;
		const client = this.#client;
		if (!identity || !client) return;
		identity.status = status;
		if (activity === undefined) delete identity.activity;
		else identity.activity = activity;
		try {
			await client.publishAsync(hubPresenceTopic(this.area, identity.agentId), JSON.stringify(identity), {
				qos: 1,
				retain: true,
			});
		} catch {
			// best-effort: the next status flip republishes
		}
	}

	/** Register the local sink for frames addressed to this identity. */
	onDelivery(sink: (msg: IrcMessage) => void): void {
		this.#deliveries = sink;
	}

	/** Invoked when the broker connection drops; hub-manager clears its cache. */
	onClose(handler: () => void): void {
		this.#onClose = handler;
	}

	/** Clear retained presence (empty payload) and end the client. */
	close(): void {
		if (this.#closed) return;
		this.#closed = true;
		const client = this.#client;
		this.#client = null;
		const identity = this.#identity;
		this.#identity = undefined;
		this.#flushPendingAcks();
		if (!client || !identity) return;
		void (async () => {
			try {
				await client.publishAsync(hubPresenceTopic(this.area, identity.agentId), Buffer.alloc(0), {
					qos: 1,
					retain: true,
				});
			} catch {
				// best-effort clear; the LWT covers an unclean death
			}
			await this.#endClient(client);
		})();
	}

	/** Route one incoming publish by topic. */
	#onMessage(topic: string, payload: Buffer): void {
		if (topic === hubFramesTopic(this.area)) {
			for (const frame of this.#frames.push(payload)) this.#onFrame(frame);
			return;
		}
		if (topic.startsWith(`hub/${this.area}/presence/`)) this.#onPresence(topic, payload);
	}

	/** Merge one retained/live presence publish into the roster map. */
	#onPresence(topic: string, payload: Buffer): void {
		const agentId = topic.slice(`hub/${this.area}/presence/`.length);
		const identity = this.#identity;
		if (!agentId || !identity) return;
		if (payload.length === 0) {
			// Peer goodbye (clean close or LWT). Our own clear cannot arrive
			// here: close() detaches #identity before publishing it.
			this.#roster.delete(agentId);
			return;
		}
		try {
			const entry = JSON.parse(payload.toString("utf8")) as HubRosterEntry;
			if (typeof entry?.agentId !== "string" || !entry.agentId) return;
			// Self-exclusion is (agentId, pid) exact: a same-id peer from
			// another process stays visible, like the unix roster.
			if (entry.agentId === identity.agentId && entry.pid === identity.pid) return;
			this.#roster.set(entry.agentId, entry);
		} catch {
			// malformed presence: ignore, keep the last known snapshot
		}
	}

	/** Dispatch one decoded frame: peer publishes deliver+ack, acks resolve. */
	#onFrame(frame: HubClientFrame | HubServerFrame): void {
		switch (frame.type) {
			case "publish": {
				this.#onPeerPublish(frame);
				break;
			}
			case "publishAck": {
				const waiter = this.#acks.get(frame.id);
				if (!waiter) return;
				for (const row of frame.results ?? []) {
					waiter.rows.set(row.to, row);
					waiter.pending.delete(row.to);
				}
				if (waiter.pending.size === 0) {
					this.#acks.delete(frame.id);
					this.#selfPublishes.delete(frame.id);
					waiter.resolve({ results: [...waiter.rows.values()] });
				}
				break;
			}
			default: {
				// welcome/roster/pong are broker-handshake frames; MQTT has no
				// broker peer, so nothing to route.
				break;
			}
		}
	}

	/** Receive-side addressing: MQTT has no broker-side fan-out, so every
	 *  area peer sees every publish frame and must decide locally, exactly
	 *  like the unix broker's target matcher (agentId + optional pid). */
	#onPeerPublish(frame: Extract<HubClientFrame, { type: "publish" }>): void {
		const identity = this.#identity;
		if (!identity) return;
		const id = typeof frame.msg?.id === "string" ? frame.msg.id : "";
		// Self-echo of our own publish (we are subscribed to the frames topic):
		// suppressed — the unix broker likewise never delivers a publish back
		// to the sending connection.
		if (id && this.#selfPublishes.has(id)) return;
		const matched = [...new Set((frame.targets ?? []).filter(target => target && hubTargetMatches(target, identity)).map(target => target.agentId))];
		if (matched.length === 0) return;
		this.#deliveries?.(frame.msg);
		const client = this.#client;
		if (!client || this.#closed) return;
		const results = matched.map(to => ({ to, ok: true }));
		void client
			.publishAsync(hubFramesTopic(this.area), encodeFrame({ type: "publishAck", id, results }), { qos: 1 })
			.catch(() => {
				// best-effort ack; the sender's request times out without it
			});
	}

	/** Resolve every in-flight publish with its partial rows (drop/close). */
	#flushPendingAcks(): void {
		for (const [id, waiter] of this.#acks) {
			this.#acks.delete(id);
			this.#selfPublishes.delete(id);
			waiter.resolve(waiter.rows.size ? { results: [...waiter.rows.values()] } : null);
		}
	}

	async #endClient(client: MqttLikeClient): Promise<void> {
		try {
			await client.endAsync(false);
		} catch {
			try {
				await client.endAsync(true);
			} catch {
				// already gone
			}
		}
	}
}

/** Extract credentials: userinfo in the URL wins; env vars otherwise. A
 *  username is the minimum (passwordless username auth exists); no username
 *  from either source means anonymous, which this transport never does. */
function resolveMqttCredentials(url: string): { username?: string; password?: string } {
	try {
		const parsed = new URL(url);
		if (parsed.username) {
			const password = decodeURIComponent(parsed.password);
			return { username: decodeURIComponent(parsed.username), password: password || undefined };
		}
	} catch {
		// unparseable URL: fall through to env; mqtt itself will fail
	}
	const username = process.env[OMP_HUB_MQTT_USERNAME_ENV]?.trim();
	const password = process.env[OMP_HUB_MQTT_PASSWORD_ENV];
	return { username: username || undefined, password: password || undefined };
}

/** Error → log-safe message. */
function errorMessage(error: unknown): string {
	return error instanceof Error ? error.message : String(error);
}
