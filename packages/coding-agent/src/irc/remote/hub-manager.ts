/**
 * Process-scoped manager for the system-scope hub client. Lazily connects on
 * first use when `hub.systemScope.enabled` is on; every failure resolves to
 * null so callers silently fall back to in-process-only behavior. Disabled or
 * unreachable means byte-identical today behavior — no socket, no daemon.
 */
import * as fs from "node:fs/promises";
import { getAgentDir, isEnoent } from "@oh-my-pi/pi-utils";
import { MAIN_AGENT_ID } from "../../registry/agent-registry";
import { HubClient } from "./client";
import { hubProjectNamespace } from "./broker";
import { resolveHubSocketPath } from "../../hub/settings";

let current: HubClient | null = null;
let starting: Promise<HubClient | null> | null = null;
let enabled = false;
let socketPath = "";
let armed = false;

/** Whether system-scope hub support is armed for this process. */
export function isHubEnabled(): boolean {
	return enabled;
}

/** Current hub client when connected; null while disabled, connecting, or unreachable. */
export function currentHubClient(): HubClient | null {
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
 * Arm the hub for this process. First write wins: subagent sessions construct
 * with their own Settings, and a later construction must never silently flip
 * the process-global hub config mid-run.
 */
export function configureHub(options: { enabled: boolean; socketPath: string }): void {
	if (armed) return;
	armed = true;
	enabled = options.enabled;
	socketPath = resolveHubSocketPath(options.socketPath, getAgentDir());
}

/**
 * Connect (and register this process under the main agent id) when armed.
 * Resolves null when disabled or unreachable within the client timeout.
 */
export async function ensureHubClient(): Promise<HubClient | null> {
	if (!enabled) return null;
	if (current) return current;
	if (!starting) {
		starting = HubClient.connect({
			socketPath,
			identity: {
				agentId: MAIN_AGENT_ID,
				project: hubProjectNamespace(process.cwd()),
				status: "running",
				pid: process.pid,
			},
		})
			.then(async client => {
				current = client;
				if (!client) return null;
				const { IrcBus } = await import("../bus");
				IrcBus.global().attachHubClient(client);
				return client;
			})
			.catch(() => null)
			.finally(() => {
				starting = null;
			});
	}
	return starting;
}

/** Detach from the broker (test seam and shutdown). */
export async function shutdownHubClient(): Promise<void> {
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
	armed = false;
	enabled = false;
	socketPath = "";
}
