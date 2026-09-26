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

/** Resolve the hub broker socket path: explicit setting, or the derived per-user default. */
export function resolveHubSocketPath(configured: string, agentDir: string): string {
	const value = configured.trim();
	return value || path.join(agentDir, "hub.sock");
}
