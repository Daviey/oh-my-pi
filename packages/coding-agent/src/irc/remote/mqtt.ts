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
	assertValidForumChannel,
	DEFAULT_REQUEST_TIMEOUT_MS,
	encodeFrame,
	FrameStream,
	hubTargetMatches,
	type ForumFrame,
	type HubClientFrame,
	type HubElectionClientFrame,
	type HubElectionServerFrame,
	type HubRosterEntry,
	type HubServerFrame,
	type HubTarget,
} from "./protocol";
import type { HubAgentIdentity, HubClientLike, HubPublishResult, HubRequestHandler, HubRequestResult } from "./client";

/** Env vars carrying broker credentials. Secrets never live in config values. */
export const OMP_HUB_MQTT_USERNAME_ENV = "OMP_HUB_MQTT_USERNAME";
export const OMP_HUB_MQTT_PASSWORD_ENV = "OMP_HUB_MQTT_PASSWORD";

/** Frames topic for an area — every peer publishes and subscribes here. */
export function hubFramesTopic(area: string): string {
	return `hub/${area}/frames`;
}

/** Retained forum topic for one channel: the latest post per channel rides
 *  here (retained, QoS1) so late joiners get one frame of context. Cleared
 *  only by the next post to the same channel — retained-until-overwritten. */
export function hubForumTopic(area: string, channel: string): string {
	return `hub/${area}/forums/${channel}`;
}

/** Presence topic for one agent under an area. Keyed by (agentId, pid) —
 *  every process owns its own retained slot, so a same-id peer's exit clear
 *  (empty payload / LWT) cannot wipe this one's roster row. The pid rides
 *  the SAME topic segment (`<agentId>:<pid>`), keeping the single-level
 *  `presence/+` wildcard subscription intact. */
export function hubPresenceTopic(area: string, agentId: string, pid: number): string {
	return `hub/${area}/presence/${agentId}:${pid}`;
}

/** How long any single client operation may take before failing open. */
const REQUEST_TIMEOUT_MS = 2_000;

/** Presence heartbeat interval for the retained-slot republish (ms). */
const HEARTBEAT_INTERVAL_MS = 30_000;
/** Outbound bytes above this for two consecutive heartbeats = stuck socket. */
const MQTT_BACKLOG_LIMIT_BYTES = 64 * 1024;
/** Minimal MqttClient surface this transport needs — keeps the fake lean. */
export interface MqttLikeClient {
	endAsync(force?: boolean): Promise<void>;
	publishAsync(topic: string, message: string | Buffer, opts?: { qos?: number; retain?: boolean }): Promise<unknown>;
	subscribeAsync(topic: string | string[], opts?: { qos?: number }): Promise<unknown>;
	on(event: "message", cb: (topic: string, payload: Buffer) => void): void;
	on(event: "close", cb: () => void): void;
	on(event: "error", cb: (err: Error) => void): void;
	/** mqtt.js exposes the underlying socket; fakes omit it. Used by the
	 *  stuck-socket watchdog — undefined simply disables the check. */
	readonly stream?: { readonly writableLength?: number } | undefined;
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
	/** In-flight RPC requests keyed by correlation id; first reply resolves. */
	#rpcWaits = new Map<string, { resolve: (result: HubRequestResult | null) => void; timer: NodeJS.Timeout }>();
	#requestSink: HubRequestHandler | undefined;
	#electionSink: ((frame: HubElectionServerFrame) => void) | undefined;
	/** Forum broadcast handlers. */
	#forumHandlers = new Set<(frame: ForumFrame) => void>();
	/** Forum frames received before any handler registered (retained forum
	 *  payloads arrive during subscribe, before onForum can be called).
	 *  Drained to the first registrant; capped as a runaway guard. */
	#forumEarlyBuffer: ForumFrame[] = [];
	/** Composite self-echo keys (from|fromSessionId|channel|ts|body). */
	#selfForumEcho = new Set<string>();
	#heartbeatTimer: NodeJS.Timeout | undefined;
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
		const presenceTopic = hubPresenceTopic(this.area, identity.agentId, identity.pid);
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
			for (const [id, waiter] of this.#rpcWaits) {
				this.#rpcWaits.delete(id);
				clearTimeout(waiter.timer);
				waiter.resolve(null);
			}
			this.#onClose?.();
		});
		connected.on("error", () => {
			// errors surface via close; nothing to do here
		});
		try {
			await connected.subscribeAsync(hubFramesTopic(this.area), { qos: 1 });
			// Retained snapshots for every present peer ride on this subscription.
			await connected.subscribeAsync(`hub/${this.area}/presence/+`, { qos: 1 });
			// Retained last-frame-per-forum-channel (late-joiner context).
			await connected.subscribeAsync(`hub/${this.area}/forums/+`, { qos: 1 });
			await connected.publishAsync(presenceTopic, JSON.stringify({ ...identity, lastSeen: Date.now() }), { qos: 1, retain: true });
			this.#startHeartbeat(presenceTopic);
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
			.publishAsync(hubFramesTopic(this.area), encodeFrame({ type: "publish", msg, targets, fromProject: this.#identity?.project, urgent: msg.urgent }), { qos: 1 })
			.catch(() => {
				// leave the timer to fail the request
			});
		return promise;
	}

	/**
	 * Transmit a request on the shared frames topic and await the first
	 * correlated reply. Every area peer sees the frame; those whose
	 * {@link onRequest} handler matches answer with a `reply` frame keyed by
	 * the correlation id. Null on timeout (no answering peer).
	 */
	async request(msg: IrcMessage, targets: HubTarget[], timeoutMs: number = DEFAULT_REQUEST_TIMEOUT_MS): Promise<HubRequestResult | null> {
		const client = this.#client;
		if (!client || this.#closed) return null;
		const id = `rpc-${globalThis.crypto.randomUUID()}`;
		const { promise, resolve } = Promise.withResolvers<HubRequestResult | null>();
		const timer = setTimeout(() => {
			this.#rpcWaits.delete(id);
			resolve(null);
		}, timeoutMs);
		timer.unref?.();
		this.#rpcWaits.set(id, { resolve, timer });
		try {
			await client.publishAsync(hubFramesTopic(this.area), encodeFrame({ type: "request", id, msg, targets, timeoutMs, fromProject: this.#identity?.project, urgent: msg.urgent }), { qos: 1 });
		} catch {
			this.#rpcWaits.delete(id);
			clearTimeout(timer);
			return null;
		}
		return promise;
	}

	/** Register the local handler for peer requests addressed to this identity. */
	onRequest(handler: HubRequestHandler | null): void {
		this.#requestSink = handler ?? undefined;
	}

	/** Register (or clear with null) the sink for election frames. */
	onElection(handler: ((frame: HubElectionServerFrame) => void) | null): void {
		this.#electionSink = handler ?? undefined;
	}

	/** Broadcast one election frame on the shared frames topic, stamped
	 *  with our agent id (MQTT has no broker to annotate `from`). */
	sendElection(frame: HubElectionClientFrame): void {
		const client = this.#client;
		if (!client || this.#closed) return;
		// sentTs: our sender clock at transmit (skew-guard input alongside
		// the unix broker's brokerTs fan-out stamp).
		void client
			.publishAsync(hubFramesTopic(this.area), encodeFrame({ ...frame, from: this.#identity?.agentId, sentTs: Date.now() }), { qos: 1 })
			.catch(() => {
				// best-effort broadcast; the next claim/beat retries
			});
	}

	/** Post to a forum channel: rides the SHARED frames topic like every
	 *  other frame (no dedicated forum topic), self-stamped with our agent
	 *  id — MQTT has no broker to annotate `from`. Fire-and-forget. */
	async forumPublish(channel: string, body: string, inReplyTo?: string): Promise<void> {
		const client = this.#client;
		assertValidForumChannel(channel);
		if (!client || this.#closed) return;
		const identity = this.#identity;
		const frame: ForumFrame = {
			kind: "forum",
			channel,
			from: identity?.agentId ?? "",
			...(identity?.sessionId ? { fromSessionId: identity.sessionId } : {}),
			body,
			ts: Date.now(),
			...(inReplyTo ? { inReplyTo } : {}),
		};
		const echoKey = `${frame.from}|${frame.fromSessionId ?? ""}|${frame.channel}|${frame.ts}|${frame.body}`;
		this.#selfForumEcho.add(echoKey);
		const encoded = encodeFrame(frame);
		void client
			.publishAsync(hubFramesTopic(this.area), encoded, { qos: 1 })
			.catch(() => {
				// best-effort broadcast; the caller re-posts if it mattered
			});
		// Retained last-frame slot for this channel: late joiners get one
		// frame of context. Retained until the next post overwrites it.
		void client
			.publishAsync(hubForumTopic(this.area, channel), encoded, { qos: 1, retain: true })
			.catch(() => {
				// best-effort context; the live frames topic carries the post
			});
	}

	/** Subscribe to forum broadcasts; the returned function unsubscribes.
	 *  The first registrant also receives any frames buffered before any
	 *  handler existed (retained forum payloads race the subscription). */
	onForum(handler: (frame: ForumFrame) => void): () => void {
		this.#forumHandlers.add(handler);
		if (this.#forumEarlyBuffer.length > 0) {
			const early = this.#forumEarlyBuffer;
			this.#forumEarlyBuffer = [];
			for (const frame of early) handler(frame);
		}
		return () => this.#forumHandlers.delete(handler);
	}

	/** Dispatch one inbound forum broadcast: skip our own echo (the unix
	 *  broker never delivers a publish back to its sender), then fan out to
	 *  every handler — no target matching anywhere on this path. */
	/** Dispatch one inbound forum broadcast: skip our own echo (keyed on
	 *  the composite frame stamp so millisecond collisions don't eat a
	 *  peer's same-ms post), then fan out — no target matching anywhere. */
	#dispatchForum(frame: ForumFrame): void {
		const echoKey = `${frame.from}|${frame.fromSessionId ?? ""}|${frame.channel}|${frame.ts}|${frame.body}`;
		if (!this.#selfForumEcho.delete(echoKey)) {
			if (this.#forumHandlers.size === 0) {
				if (this.#forumEarlyBuffer.length < 100) this.#forumEarlyBuffer.push(frame);
				return;
			}
			for (const handler of this.#forumHandlers) handler(frame);
		}
	}

	/** Presence heartbeat: republish the retained slot with a fresh
	 *  lastSeen stamp every interval (roster freshness on the area). */
	#startHeartbeat(presenceTopic: string): void {
		clearInterval(this.#heartbeatTimer);
		let stuckChecks = 0;
		this.#heartbeatTimer = setInterval(() => {
			const identity = this.#identity;
			const client = this.#client;
			if (!identity || !client) return;
			// Stuck-socket watchdog: a broker that stopped reading this session
			// (keepalive kills observed server-side) leaves a half-open TCP peer
			// that never emits "close", while publishes pile into the kernel
			// send buffer and the client spins. Two consecutive heartbeats with
			// a full outbound buffer means dead: force-destroy; the close
			// handler fires and the retry ladder reconnects fresh.
			const backlog = client.stream?.writableLength;
			if (backlog !== undefined && backlog > MQTT_BACKLOG_LIMIT_BYTES) {
				stuckChecks += 1;
				if (stuckChecks >= 2) {
					logger.warn("hub mqtt: outbound buffer stuck, forcing reconnect", {
						backlog,
						area: this.area,
					});
					// Stop the interval before destroying: the close event only
					// flushes waiters; a fresh client owns the next heartbeat.
					clearInterval(this.#heartbeatTimer);
					this.#heartbeatTimer = undefined;
					void client.endAsync(true).catch(() => undefined);
					return;
				}
				// Don't pile another retained publish onto a full buffer.
				return;
			}
			stuckChecks = 0;
			void client
				.publishAsync(presenceTopic, JSON.stringify({ ...identity, lastSeen: Date.now() }), { qos: 1, retain: true })
				.catch(() => {
					// best-effort: the next interval retries
				});
		}, HEARTBEAT_INTERVAL_MS);
		this.#heartbeatTimer.unref?.();
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
			await client.publishAsync(hubPresenceTopic(this.area, identity.agentId, identity.pid), JSON.stringify(identity), {
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
		clearInterval(this.#heartbeatTimer);
		this.#heartbeatTimer = undefined;
		const client = this.#client;
		this.#client = null;
		const identity = this.#identity;
		this.#identity = undefined;
		this.#flushPendingAcks();
		for (const [id, waiter] of this.#rpcWaits) {
			this.#rpcWaits.delete(id);
			clearTimeout(waiter.timer);
			waiter.resolve(null);
		}
		if (!client || !identity) return;
		void (async () => {
			try {
				await client.publishAsync(hubPresenceTopic(this.area, identity.agentId, identity.pid), Buffer.alloc(0), {
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
		// Retained last-frame-per-channel: SAME ingest path as live frames
		// (self-echo suppression keys on the frame stamp, so the sender's
		// own retained echo does not double-fire).
		if (topic.startsWith(`hub/${this.area}/forums/`)) {
			for (const frame of this.#frames.push(payload)) this.#onFrame(frame);
			return;
		}
		if (topic.startsWith(`hub/${this.area}/presence/`)) this.#onPresence(topic, payload);
	}

	/** Merge one retained/live presence publish into the roster map. Rows are
	 *  keyed by (agentId, pid) — several processes share one agent id (every
	 *  process's main agent is "Main"), and each owns its own retained slot,
	 *  so the map holds one row per process exactly like the unix broker's
	 *  per-connection roster. The entry's own payload is authoritative; the
	 *  topic suffix only keys the delete on empty (clear) payloads. */
	#onPresence(topic: string, payload: Buffer): void {
		const suffix = topic.slice(`hub/${this.area}/presence/`.length);
		const identity = this.#identity;
		if (!suffix || !identity) return;
		if (payload.length === 0) {
			// Peer goodbye (clean close or LWT) — clears only the dying
			// process's own slot: the topic is (agentId, pid)-keyed, so a
			// same-id peer's clear cannot wipe this row. Our own clear cannot
			// arrive here: close() detaches #identity before publishing it.
			this.#roster.delete(suffix);
			return;
		}
		try {
			const entry = JSON.parse(payload.toString("utf8")) as HubRosterEntry;
			if (typeof entry?.agentId !== "string" || !entry.agentId) return;
			// Self-exclusion is (agentId, pid) exact: a same-id peer from
			// another process stays visible, like the unix roster.
			if (entry.agentId === identity.agentId && entry.pid === identity.pid) return;
			this.#roster.set(`${entry.agentId}:${entry.pid}`, entry);
		} catch {
			// malformed presence: ignore, keep the last known snapshot
		}
	}

	/** Dispatch one decoded frame: peer publishes deliver+ack, acks resolve. */
	#onFrame(frame: HubClientFrame | HubServerFrame): void {
		// Forum posts bypass hubTargetMatches entirely: every area peer sees
		// them (broadcast, not addressed); self-echo suppressed in #dispatchForum.
		if (frame.type === undefined) {
			this.#dispatchForum(frame);
			return;
		}
		switch (frame.type) {
			case "publish": {
				this.#onPeerPublish(frame);
				break;
			}
			case "request": {
				if ("targets" in frame) this.#onPeerRequest(frame);
				break;
			}
			case "reply": {
				const waiter = this.#rpcWaits.get(frame.id);
				if (!waiter) return; // late/duplicate reply: already resolved
				this.#rpcWaits.delete(frame.id);
				clearTimeout(waiter.timer);
				waiter.resolve({ from: frame.from, msg: frame.msg });
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
			case "leaderClaim":
			case "leaderAbdicate":
			case "heartbeat": {
				// Election broadcasts bypass target matching entirely: every
				// area peer sees them; heartbeats refresh roster rows.
				if (frame.type === "heartbeat" && frame.from && Number.isFinite(frame.lastSeen)) {
					for (const entry of this.#roster.values()) {
						if (entry.agentId === frame.from) entry.lastSeen = frame.lastSeen;
					}
				}
				this.#electionSink?.(frame);
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
		// Bare targets (no project) scope to the sender's namespace — same
		// contract as the unix broker's conn.project resolution. A bare
		// target from a different project does not match (cross-talk guard).
		const resolved = (frame.targets ?? []).map(target =>
			target && target.project === undefined && typeof frame.fromProject === "string"
				? { ...target, project: frame.fromProject }
				: target,
		);
		const matched = [...new Set(resolved.filter(target => target && hubTargetMatches(target, identity)).map(target => target.agentId))];
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

	/** Receive-side addressing for requests (same matcher as publish): the
	 *  handler's answer goes back on the frames topic keyed by correlation
	 *  id. A null/throwing handler declines — no reply frame is published. */
	#onPeerRequest(frame: Extract<HubClientFrame, { type: "request" }>): void {
		const identity = this.#identity;
		const handler = this.#requestSink;
		if (!identity || !handler) return;
		if (this.#selfPublishes.has(frame.id)) return; // own request echo
		const resolved = (frame.targets ?? []).map(target =>
			target && target.project === undefined && typeof frame.fromProject === "string"
				? { ...target, project: frame.fromProject }
				: target,
		);
		const matched = resolved.some(target => target && hubTargetMatches(target, identity));
		if (!matched) return;
		void (async () => {
			let answer: IrcMessage | null = null;
			try {
				answer = (await handler(frame.msg, frame.msg?.from ?? "")) ?? null;
			} catch (error) {
				// A throwing handler is a distinct failure, not a decline: the
				// requester must learn fast instead of waiting out its whole
				// RPC timeout for a reply that will never come.
				answer = {
					id: `rpc-err-${frame.id}`,
					from: identity.agentId,
					to: frame.msg?.from ?? "",
					body: `[rpc-error: peer handler failed — ${errorMessage(error)}]`,
					ts: Date.now(),
					replyTo: frame.id,
				};
			}
			const client = this.#client;
			if (!answer || !client || this.#closed) return;
			try {
				await client.publishAsync(
					hubFramesTopic(this.area),
					encodeFrame({ type: "reply", id: frame.id, from: identity.agentId, msg: answer }),
					{ qos: 1 },
				);
			} catch {
				// best-effort answer; the requester times out without it
			}
		})();
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
