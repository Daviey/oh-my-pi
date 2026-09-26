import type { AgentToolResult } from "@oh-my-pi/pi-agent-core";
import type { IrcMessage } from "@oh-my-pi/pi-tui/tools/irc";
import type { CoordinationDetails } from "@oh-my-pi/pi-tui/tools/wait";
import type { Settings } from "../config/settings";
import { IrcBus } from "./bus";
import { currentHubClient, hubRoster, ensureHubClient, isHubEnabled } from "./remote/hub-manager";
import { hubProjectNamespace } from "./remote/broker";
import type { HubTarget } from "./remote/protocol";
import type { AgentRef } from "../registry/agent-registry";
import type { HubRosterRow } from "./remote/hub-manager";
import { type AgentRegistry, MAIN_AGENT_ID } from "../registry/agent-registry";
import { ensurePersistedRoster } from "../registry/persisted-agents";
import { canSpawnAtDepth } from "../task/types";

import { cfgTaskMaxRecursionDepth } from "../task/settings";

function coordinationErrorResult(text: string, details: CoordinationDetails): AgentToolResult<CoordinationDetails> {
	return { content: [{ type: "text", text }], details, isError: true };
}

/** Messaging is available to subagents and to top-level sessions able to spawn peers. */
export function isIrcEnabled(settings: Settings, taskDepth: number): boolean {
	if (taskDepth > 0) return true;
	const maxDepth = cfgTaskMaxRecursionDepth.get(settings);
	return canSpawnAtDepth(maxDepth, taskDepth);
}

export function formatIncoming(msg: IrcMessage): string {
	const replyTag = msg.replyTo ? ` (reply to ${msg.replyTo})` : "";
	return `[${msg.id}] ${msg.from}${replyTag}: ${msg.body}`;
}

/** Session-buffered inbox drain used before parking a bus waiter. */
export function drainPendingInbox(registry: AgentRegistry, senderId: string, from?: string): IrcMessage | undefined {
	const session = registry.get(senderId)?.session;
	return typeof session?.drainPendingIrcInboxMessages === "function"
		? session.drainPendingIrcInboxMessages(senderId, { from, limit: 1 })[0]
		: undefined;
}

/** `wait` result carrying a consumed message. */
export function messageResult(senderId: string, waited: IrcMessage): AgentToolResult<CoordinationDetails> {
	return {
		content: [{ type: "text", text: formatIncoming(waited) }],
		details: { op: "wait", from: senderId, waited },
	};
}

/** Send a direct message or broadcast; delivery never waits for a reply. */
export async function executeSend(
	deps: { registry: AgentRegistry; senderId: string; sessionFileHint?: string | null },
	params: { to: string; message: string },
): Promise<AgentToolResult<CoordinationDetails>> {
	const { registry, senderId, sessionFileHint } = deps;
	const to = params.to.trim();
	const message = params.message;
	if (!to) return coordinationErrorResult("A recipient is required.", { op: "send", from: senderId });
	if (!message.trim())
		return coordinationErrorResult("A non-empty message is required.", { op: "send", from: senderId });
	if (to === senderId)
		return coordinationErrorResult("Cannot send a message to yourself.", { op: "send", from: senderId, to });
	const isBroadcast = to === "all";
	const projectScoped = parseProjectScope(to);
	// Restore parked recipients only when needed; never delay delivery to a live peer.
	if (!isBroadcast && sessionFileHint) {
		const recipient = registry.get(to);
		if (!recipient || recipient.status === "parked") await ensurePersistedRoster(registry, sessionFileHint);
	}

	if (isHubEnabled()) await ensureHubClient();
	const remotePeers = await hubRoster();
	// Unqualified broadcast stays within this project namespace: only same-project
	// broker peers join the registry overlay; cross-project needs `project:` scope.
	const ownNamespace = hubProjectNamespace(process.cwd());
	const sameProjectPeers = remotePeers.filter(row => row.project === ownNamespace);
	if (sameProjectPeers.length > 0) registry.setHubPeers(hubRowsToRefs(sameProjectPeers));

	if (projectScoped) {
		return sendProjectScoped({ senderId, message, scope: projectScoped, remotePeers });
	}

	const targets = isBroadcast ? registry.listVisibleTo(senderId).map(ref => ref.id) : [to];
	const suppressRelay = isBroadcast && targets.includes(MAIN_AGENT_ID);
	const bus = IrcBus.global();
	const receipts = await Promise.all(
		targets.map(target => bus.send({ from: senderId, to: target, body: message }, { suppressRelay })),
	);
	const delivered = receipts.filter(receipt => receipt.outcome !== "failed");
	let text: string;
	if (isBroadcast) {
		text =
			targets.length === 0
				? "No live peers to broadcast to."
				: `Broadcast delivered to ${delivered.length} of ${targets.length} peer(s).`;
		if (receipts.length) {
			text += `\n${receipts
				.map(receipt =>
					receipt.outcome === "failed"
						? `- ${receipt.to}: failed — ${receipt.error ?? "not running"}`
						: `- ${receipt.to}: ${receipt.outcome}`,
				)
				.join("\n")}`;
		}
	} else {
		const receipt = receipts[0]!;
		const recipient = registry.get(to);
		const unavailable = !recipient || recipient.status === "aborted" || !recipient.session;
		text =
			receipt.outcome === "failed"
				? `Failed: ${to} ${unavailable ? "is not running" : "could not receive the message"}. ${receipt.error ?? ""}`.trimEnd()
				: receipt.outcome === "revived"
					? `Queued for ${to} (was parked; revived).`
					: `Delivered to ${to}.`;
	}
	return {
		content: [{ type: "text", text }],
		details: { op: "send", from: senderId, to, receipts },
		isError: delivered.length === 0 && targets.length > 0,
	};
}

/**
 * Parse a `project:<ns>:all` / `project:<ns>:<agentId>` scope qualifier.
 * `all` (unqualified) stays within the sender's own project namespace.
 */
function parseProjectScope(to: string): { project: string; agentId: string } | null {
	const match = /^project:([^:]+):(.+)$/.exec(to);
	if (!match) return null;
	return { project: match[1]!, agentId: match[2]! };
}

/** Broker roster rows merged into the registry's remote peer overlay. */
function hubRowsToRefs(rows: HubRosterRow[]): AgentRef[] {
	return rows.map(row => ({
		id: row.agentId,
		displayName: row.agentId,
		kind: "sub" as const,
		status: row.status,
		session: null,
		sessionFile: row.sessionFile ?? null,
		createdAt: 0,
		lastActivity: 0,
	}));
}

/**
 * Deliver a message to one cross-project recipient, or broadcast within a
 * namespace (`project:<ns>:all`). Delivery reuses the hub publish path; the
 * recipient's process runs its own injected/woken/revived machinery.
 */
async function sendProjectScoped(deps: {
	senderId: string;
	message: string;
	scope: { project: string; agentId: string };
	remotePeers: HubRosterRow[];
}): Promise<AgentToolResult<CoordinationDetails>> {
	const { senderId, message, scope, remotePeers } = deps;
	const client = currentHubClient();
	const target: HubTarget = { project: scope.project, agentId: scope.agentId };
	const roster =
		scope.agentId === "all"
			? remotePeers.filter(row => row.project === scope.project && row.agentId !== senderId)
			: remotePeers.filter(row => row.project === scope.project && row.agentId === scope.agentId);
	if (!client || roster.length === 0) {
		const reason = !client
			? "the system-scope hub is not connected"
			: `no peer "${scope.agentId}" in project "${scope.project}"`;
		return coordinationErrorResult(`Failed: ${reason}.`, {
			op: "send",
			from: senderId,
			to: `project:${scope.project}:${scope.agentId}`,
		});
	}
	const hubMessage = {
		from: senderId,
		to: scope.agentId,
		body: message,
		id: `${senderId}-${Date.now()}`,
		ts: Date.now(),
	};
	const targets =
		scope.agentId === "all" ? roster.map(row => ({ project: row.project, agentId: row.agentId })) : [target];
	const result = await client.publish(hubMessage, targets);
	const results =
		result?.results ?? targets.map(entry => ({ to: entry.agentId, ok: false, error: "hub publish failed" }));
	const delivered = results.filter(entry => entry.ok);
	const text =
		targets.length > 1
			? `Broadcast delivered to ${delivered.length} of ${targets.length} peer(s) in project ${scope.project}.`
			: delivered.length > 0
				? `Delivered to ${scope.agentId} in project ${scope.project}.`
				: `Failed: ${results[0]?.error ?? "hub publish failed"}.`;
	return {
		content: [{ type: "text", text }],
		details: { op: "send", from: senderId, to: `project:${scope.project}:${scope.agentId}`, receipts: [] },
		isError: delivered.length === 0,
	};
}

/** Namespace qualifier for this process's own project directory. */
export function ownProjectNamespace(): string {
	return hubProjectNamespace(process.cwd());
}
