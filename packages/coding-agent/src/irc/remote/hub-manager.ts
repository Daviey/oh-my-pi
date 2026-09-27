/**
 * Process-scoped manager for the system-scope hub client. When
 * `hub.systemScope.enabled` is on, sessions eager-subscribe at startup
 * (fire-and-forget ensureHubClient) and lazy ops fall back to connect-on-use;
 * a startup-race loser climbs a bounded retry ladder (1s/5s/30s). Every
 * failure resolves to null so callers silently fall back to in-process-only
 * behavior. Disabled or unreachable means byte-identical today behavior —
 * no socket, no daemon.
 */
import * as path from "node:path";
import * as fs from "node:fs/promises";
import { getAgentDir, isEnoent } from "@oh-my-pi/pi-utils";
import { logger } from "@oh-my-pi/pi-utils";
import { AgentRegistry, MAIN_AGENT_ID } from "../../registry/agent-registry";
import { HubClient, type HubClientLike } from "./client";
import { MqttHubClient } from "./mqtt";
import { hubProjectNamespace } from "./broker";
import { IrcBus } from "../bus";
import { redactHubUrl, resolveHubArea, resolveHubSocketPath, resolveHubTransport } from "../../hub/settings";

let current: HubClientLike | null = null;
let starting: Promise<HubClientLike | null> | null = null;
let enabled = false;
let socketPath = "";
let armed = false;

/** Whether system-scope hub support is armed for this process. */
export function isHubEnabled(): boolean {
	return enabled;
}

/** Current hub client when connected; null while disabled, connecting, or unreachable. */
export function currentHubClient(): HubClientLike | null {
	return current;
}

/** Broker roster of agents from OTHER processes (own entries excluded). */
export async function hubRoster(): Promise<HubRosterRow[]> {
	const client = current;
	if (!client) return [];
	const roster = await client.roster();
	return roster.map(entry => ({ ...entry, remote: true as const }));
}

/** A broker roster row extended for registry merging. */
export interface HubRosterRow {
	agentId: string;
	project: string;
	status: "running" | "idle";
	pid: number;
	sessionFile?: string;
	remote: true;
}

/**
 * Arm the hub for this process. Disabled→enabled transitions are allowed
 * (session construction order is not guaranteed main-first, so an early
 * disabled write must not permanently lock the hub off); enabled→disabled
 * is refused — a later session can never silently tear the hub down mid-run.
 */
export function configureHub(options: {
	enabled: boolean;
	socketPath: string;
	transport?: string;
	remoteUrl?: string;
	/** Named topic namespace for non-unix transports (resolved + stored). */
	area?: string;
}): void {
	if (armed && (enabled || !options.enabled)) return;
	armed = true;
	enabled = options.enabled;
	socketPath = resolveHubSocketPath(options.socketPath, getAgentDir());
	transport = resolveHubTransport(options.transport ?? "unix");
	remoteUrl = options.remoteUrl?.trim() || "";
	area = resolveHubArea(options.area ?? "");
}

/**
 * Eager-connect retry state: a session that loses the startup race (spawn
 * deadline hit while another process's broker is still binding) must not
 * stay off the bus forever. Bounded backoff; success resets the ladder.
 */
const RETRY_DELAYS_MS = [1_000, 5_000, 30_000];
let retryAttempt = 0;
let retryTimer: ReturnType<typeof setTimeout> | undefined;
/** Resolved transport (fail-closed for unimplemented kinds — see resolveHubTransport). */
let transport: { kind: string; implemented: boolean } = { kind: "unix", implemented: true };
let remoteUrl = "";
/** Resolved hub area (named topic namespace for mqtt). */
let area = "";

function scheduleHubRetry(): void {
	if (!enabled || current || starting) return;
	if (retryAttempt >= RETRY_DELAYS_MS.length) return; // give up; lazy ops still work
	const delay = RETRY_DELAYS_MS[retryAttempt++];
	retryTimer = setTimeout(() => {
		retryTimer = undefined;
		void ensureHubClient();
	}, delay);
	retryTimer.unref?.();
}

/**
 * Connect (and register this process under the main agent id) when armed.
 * Resolves null when disabled or unreachable within the client timeout.
 */
export async function ensureHubClient(): Promise<HubClientLike | null> {
	if (!enabled) return null;
	if (current) return current;
	// Fail-closed for schema-declared-but-unimplemented transports: config is
	// accepted, connection is refused with a typed log — never a silent unix
	// fallback that would leak cross-system intents onto a local-only bus.
	if (!transport.implemented) {
		logger.warn("hub: transport not implemented; hub disabled for this session", {
			kind: transport.kind,
			remoteUrl: remoteUrl ? redactHubUrl(remoteUrl) : "(unset)",
		});
		enabled = false;
		return null;
	}
	if (transport.kind === "mqtt" && !remoteUrl) {
		// Implemented transport without its required URL: same fail-closed
		// shape (typed log, self-disable) so retry ladders never spin on a
		// config that can never connect.
		logger.warn("hub: mqtt transport requires hub.remoteUrl; hub disabled for this session");
		enabled = false;
		return null;
	}
	if (!starting) {
		// See RETRY_DELAYS_MS below: failures back off, successes reset.
		starting = connectTransport()
			.then(async client => {
				if (client) {
					retryAttempt = 0;
					attachHubClient(client);
				} else {
					// Startup-race loser (spawn deadline hit): retry on backoff
					// instead of staying unsubscribed until a manual hub op.
					scheduleHubRetry();
				}
				current = client;
				return client;
			})
			.catch(() => null)
			.finally(() => {
				starting = null;
			});
	}
	return starting;
}

/** Connect via the armed transport (unix default; mqtt when configured). */
function connectTransport(): Promise<HubClientLike | null> {
	if (transport.kind === "mqtt") {
		return MqttHubClient.connect({ url: remoteUrl, area, identity: mainIdentity() });
	}
	return HubClient.connect({ socketPath, identity: mainIdentity() });
}

/** This process's registration under the main agent id. */
function mainIdentity() {
	return {
		agentId: MAIN_AGENT_ID,
		project: hubProjectNamespace(process.cwd()),
		status: "running" as const,
		pid: process.pid,
		// Main agents are generalists; their project context IS their
		// static specialism (answers "who is working on X" from the
		// roster alone). Spawned specialists set their own via task name.
		specialism: path.basename(path.resolve(process.cwd())) || undefined,
	};
}

/** Post-connect wiring shared by every transport: bus attachment, status
 *  mirror, and the drop-and-retry handler for a dead broker connection. */
function attachHubClient(client: HubClientLike): void {
	IrcBus.global().attachHubClient(client);
	startStatusSync(client);
	// Broker died (idle-exit/crash): drop the cached client and
	// schedule a reconnect — a session must not silently fall
	// off the bus after one disconnect.
	client.onClose(() => {
		if (current === client) current = null;
		scheduleHubRetry();
	});
}

/** Active registry→hub status mirror; replaced on reconnect, torn down on shutdown. */
let statusSyncUnsubscribe: (() => void) | undefined;

/** Minimum interval between wire status frames (per agent): activity gists
 *  change per tool call — on-change-only with this floor keeps 20 sessions
 *  from chattiness while staying fresh. */
const STATUS_SYNC_MIN_INTERVAL_MS = 5_000;

/** Mirror this process's main-agent registry ref into hub status frames:
 *  run-state flips immediately, activity gist debounced (on-change, floor). */
function startStatusSync(client: HubClientLike): void {
	statusSyncUnsubscribe?.();
	const lastSent = new Map<string, { status: string; activity?: string; at: number }>();
	const unsub = AgentRegistry.global().onChange(event => {
		if (event.type !== "status_changed" && event.type !== "metadata_changed") return;
		const ref = event.ref;
		if (ref.id !== MAIN_AGENT_ID) return;
		const prev = lastSent.get(ref.id);
		const now = Date.now();
		const statusChanged = !prev || prev.status !== ref.status;
		const activityChanged = (prev?.activity ?? undefined) !== (ref.activity ?? undefined);
		if (!statusChanged && !activityChanged) return;
		// Status flips go immediately (delivery routing depends on them);
		// activity-only updates respect the debounce floor.
		if (!statusChanged && prev && now - prev.at < STATUS_SYNC_MIN_INTERVAL_MS) return;
		lastSent.set(ref.id, { status: ref.status, activity: ref.activity, at: now });
		// Terminal states (aborted/parked) have no hub equivalent: the process
		// is about to leave the roster entirely (bye/socket close).
		if (ref.status === "running" || ref.status === "idle") {
			void client.setStatus(ref.status, ref.activity).catch(() => {});
		}
	});
	statusSyncUnsubscribe = () => {
		unsub();
		lastSent.clear();
	};
}

/** Detach from the broker (test seam and shutdown). */
export async function shutdownHubClient(): Promise<void> {
	statusSyncUnsubscribe?.();
	statusSyncUnsubscribe = undefined;
	if (retryTimer) {
		clearTimeout(retryTimer);
		retryTimer = undefined;
	}
	const client = current;
	current = null;
	enabled = false;
	client?.close();
}

/** Ensure the derived default socket's parent directory exists (agent dir). */
export async function ensureHubSocketParent(): Promise<void> {
	try {
		await fs.mkdir(getAgentDir(), { recursive: true, mode: 0o700 });
	} catch (error) {
		if (!isEnoent(error)) throw error;
	}
}

/** Test-only: reset the armed process-global hub config. */
export function resetHubForTests(): void {
	if (retryTimer) {
		clearTimeout(retryTimer);
		retryTimer = undefined;
	}
	retryAttempt = 0;
	armed = false;
	enabled = false;
	socketPath = "";
	transport = { kind: "unix", implemented: true };
	remoteUrl = "";
	area = "";
}
