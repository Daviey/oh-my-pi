/**
 * Wire protocol for the system-scope hub broker: newline-delimited JSON frames
 * over a per-user unix socket. Only same-uid peers may connect (enforced by
 * the broker via SO_PEERCRED plus 0600 socket permissions); there is no TCP.
 */
import type { AgentStatus } from "@oh-my-pi/pi-tui/overlays/agent-hub-types";
import type { IrcMessage } from "@oh-my-pi/pi-tui/tools/irc";

/** Broker idle grace env (ms without a connected peer before the daemon exits). */
export const HUB_IDLE_GRACE_ENV = "OMP_HUB_IDLE_GRACE_MS";
/** Socket path env passed to the spawned broker daemon. */
export const HUB_SOCKET_PATH_ENV = "OMP_HUB_SOCKET_PATH";

/** Default ms without any connected peer before the broker daemon exits. */
export const DEFAULT_HUB_IDLE_GRACE_MS = 30_000;

/** One roster row: an agent visible to the whole user's machine. */
export interface HubRosterEntry {
	agentId: string;
	/** Project namespace (wyhash hex of the canonical project directory). */
	project: string;
	status: Extract<AgentStatus, "running" | "idle">;
	pid: number;
	sessionFile?: string;
}

/** A specific cross-project recipient: explicit namespace, or the sender's own when omitted. */
export interface HubTarget {
	project?: string;
	agentId: string;
}

/** Client → broker frames. */
export type HubClientFrame =
	| { type: "hello"; agents: HubRosterEntry[] }
	| { type: "status"; agentId: string; status: "running" | "idle" }
	| { type: "roster" }
	| { type: "publish"; msg: IrcMessage; targets: HubTarget[] }
	| { type: "ping" }
	| { type: "bye" };

/** Broker → client frames. */
export type HubServerFrame =
	| { type: "welcome"; self: string; roster: HubRosterEntry[] }
	| { type: "roster"; roster: HubRosterEntry[] }
	| { type: "deliver"; msg: IrcMessage }
	| { type: "publishAck"; id: string; results: { to: string; ok: boolean; error?: string }[] }
	| { type: "pong" }
	| { type: "bye" }
	| { type: "error"; message: string };

/** Encode one frame as a newline-terminated JSON line. */
export function encodeFrame(frame: HubClientFrame | HubServerFrame): string {
	return `${JSON.stringify(frame)}\n`;
}

/** Incremental NDJSON frame parser shared by client and broker connections. */
export class FrameStream {
	#buffer = "";
	push(chunk: string | Buffer): (HubClientFrame | HubServerFrame)[] {
		this.#buffer += typeof chunk === "string" ? chunk : chunk.toString("utf8");
		const frames: (HubClientFrame | HubServerFrame)[] = [];
		let index: number;
		while ((index = this.#buffer.indexOf("\n")) !== -1) {
			const line = this.#buffer.slice(0, index).trim();
			this.#buffer = this.#buffer.slice(index + 1);
			if (!line) continue;
			try {
				frames.push(JSON.parse(line));
			} catch {
				frames.push({ type: "error", message: "malformed frame" });
			}
		}
		return frames;
	}
}
