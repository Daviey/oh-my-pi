/**
 * Settings declared by this domain (see `config/registry.ts`). Declaration order is the
 * settings-panel order; `config/all-settings.ts` registers every domain.
 */
import * as path from "node:path";
import { register } from "../config/registry";

// Hub (system-scope IRC broker)
export const cfgHubSystemScopeEnabled = register({
	id: "hub.systemScope.enabled",
	type: "boolean",
	default: false,
	ui: {
		tab: "interaction",
		group: "Hub",
		label: "System-Scope Hub",
		description:
			"Relay agent IRC across omp processes on this machine via a per-user broker daemon. " +
			"SECURITY: enabling lets any same-uid local process message and steer your agents. " +
			"Off (default) keeps IRC strictly in-process.",
	},
});

export const cfgHubTransport = register({
	id: "hub.transport",
	type: "string",
	default: "unix",
	ui: {
		tab: "interaction",
		group: "Hub",
		label: "Hub Transport",
		description:
			"unix = built-in same-host broker (default). mqtt / redis = bridge the local broker to a " +
			"network broker for cross-system scopes. Non-unix transports are schema-declared but not " +
			"implemented yet: config is accepted, connection fails closed with a logged error.",
	},
});

/** Remote-broker connection target for non-unix transports (mqtt://host:port,
 *  redis://host:port; empty = transport disabled even when kind is set). */
export const cfgHubRemoteUrl = register({
	id: "hub.remoteUrl",
	type: "string",
	default: "",
	ui: {
		tab: "interaction",
		group: "Hub",
		label: "Hub Remote Broker URL",
		description:
			"Connection URL for the network broker (mqtt://host:port or redis://host:port). " +
			"Required when hub.transport is not unix; empty keeps the hub same-host.",
	},
});

export const cfgHubSystemScopeSocketPath = register({
	id: "hub.systemScope.socketPath",
	type: "string",
	default: "",
	ui: {
		tab: "interaction",
		group: "Hub",
		label: "Hub Socket Path",
		description:
			"Unix socket for the system-scope hub broker; empty derives ~/.omp/agent/hub.sock. " +
			"A custom path whose parent directory is group- or world-writable is refused at bind; " +
			"the socket itself is created 0600. Setting a path alone enables nothing.",
	},
});

/** Hub transports. `unix` = the built-in per-user broker over a local unix
 *  socket (SO_PEERCRED trust, same-host only). The remote kinds (mqtt, redis)
 *  bridge the local broker to a network broker so scopes extend across
 *  systems — schema v1's versioning/capabilities gate that path. New kinds
 *  are additive: unknown configured values fail closed at connect. */
export type HubTransportKind = "unix" | "mqtt" | "redis";

export const HUB_TRANSPORT_KINDS: readonly HubTransportKind[] = ["unix", "mqtt", "redis"];

/** Resolve the hub broker socket path: explicit setting, or the derived per-user default. */
export function resolveHubSocketPath(configured: string, agentDir: string): string {
	const value = configured.trim();
	return value || path.join(agentDir, "hub.sock");
}

/** Validate a configured transport kind; `unix` is the only implemented kind
 *  today — others are accepted in config (forward-declared schema) but the
 *  client refuses to connect and logs a typed error, fail-closed. */
export function resolveHubTransport(kind: string): { kind: HubTransportKind; implemented: boolean } {
	const value = kind.trim().toLowerCase();
	if (!HUB_TRANSPORT_KINDS.includes(value as HubTransportKind)) {
		// Unknown configured value: fail CLOSED on the DECLARED kind (never a
		// silent unix fallback that would leak cross-system intents locally);
		// `implemented:false` makes ensureHubClient refuse to connect.
		return { kind: value as HubTransportKind, implemented: false };
	}
	return { kind: value as HubTransportKind, implemented: value === "unix" };
}
