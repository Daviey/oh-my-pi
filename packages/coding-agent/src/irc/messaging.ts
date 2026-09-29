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
	params: { to: string; message: string; messageId?: string; urgent?: boolean; replyTo?: string },
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
		return sendProjectScoped({
			senderId,
			message,
			scope: projectScoped,
			remotePeers,
			messageId: params.messageId,
			urgent: params.urgent,
			replyTo: params.replyTo,
		});
	}

	const systemScoped = parseSystemScope(to);
	if (systemScoped) {
		return sendSystemScoped({
			senderId,
			message,
			agentId: systemScoped.agentId,
			remotePeers,
			messageId: params.messageId,
			urgent: params.urgent,
			replyTo: params.replyTo,
		});
	}

	const pidScoped = parsePidScope(to);
	if (pidScoped) {
		return sendPidScoped({
			senderId,
			message,
			pid: pidScoped.pid,
			agentId: pidScoped.agentId,
			remotePeers,
			urgent: params.urgent,
			messageId: params.messageId,
			replyTo: params.replyTo,
		});
	}

	const sessionScoped = parseSessionScope(to);
	if (sessionScoped) {
		return sendSessionScoped({
			senderId,
			message,
			sessionId: sessionScoped.sessionId,
			agentId: sessionScoped.agentId,
			remotePeers,
			messageId: params.messageId,
			urgent: params.urgent,
			replyTo: params.replyTo,
		});
	}

	const targets = isBroadcast ? registry.listVisibleTo(senderId).map(ref => ref.id) : [to];
	const suppressRelay = isBroadcast && targets.includes(MAIN_AGENT_ID);
	const bus = IrcBus.global();
	const receipts = await Promise.all(
		targets.map(target => bus.send({ from: senderId, to: target, body: message, urgent: params.urgent, replyTo: params.replyTo, ...(params.messageId ? { id: params.messageId } : {}) }, { suppressRelay })),
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

/** Visible for tests: system/global scope syntax. */
export function parseSystemScopeForTest(to: string): { agentId: string } | null {
	return parseSystemScope(to);
}

/** Visible for tests: session scope syntax. */
export function parseSessionScopeForTest(to: string): { sessionId: string; agentId: string } | null {
	return parseSessionScope(to);
}

/** Reply-correlated request: send, then poll the sender's mailbox for a
 *  message whose `replyTo` equals the sent id. Fail-fast roster check first
 *  (dead peer → immediate error), deadline-bounded wait after (a busy peer's
 *  inject surfaces at its tool boundary — the deadline must exceed its max
 *  tool-call length). This is the synchronous half of cross-session RPC;
 *  the reply side is an ordinary `irc send` with `replyTo` set. */
export async function executeRequest(
	deps: { registry: AgentRegistry; senderId: string; sessionFileHint?: string | null },
	params: { to: string; message: string; timeoutMs?: number; urgent?: boolean },
): Promise<AgentToolResult<CoordinationDetails>> {
	const { senderId } = deps;
	const to = params.to.trim();
	const message = params.message;
	if (!to) return coordinationErrorResult("A recipient is required.", { op: "request", from: senderId });
	if (!message.trim())
		return coordinationErrorResult("A non-empty message is required.", { op: "request", from: senderId, to });
	if (to === senderId)
		return coordinationErrorResult("Cannot request from yourself.", { op: "request", from: senderId, to });

	// No cheap liveness check beats sending: executeSend already fail-fasts on
	// unknown peers (roster + registry), so reuse it and let its errors
	// short-circuit before any waiting starts. The wire id IS the correlation
	// id: the replier answers with `replyTo: "<requestId>"` (rendered in the
	// incoming inject).
	const requestId = `${senderId}-req-${Date.now()}`;
	const timeoutMs = Math.max(1_000, params.timeoutMs ?? 300_000);
	const deadline = Date.now() + timeoutMs;
	const bus = IrcBus.global();
	// Subscribe BEFORE the send: a fast reply (~300ms steer latency) can land
	// while executeSend still awaits publish acks (up to 2s). A successful
	// reply is consumed by the peer's session (steer/wake injection) and never
	// buffers to the mailbox — without this observer the wait below can only
	// ever see FAILED deliveries. The predicate mirrors takeMatching's; first
	// match wins and unregisters.
	let deliveredReply: IrcMessage | undefined;
	const unsubscribe = bus.onDeliver(candidate => {
		if (deliveredReply) return;
		if (candidate.replyTo === requestId) deliveredReply = candidate;
	});
	try {
		const sendResult = await executeSend(deps, { to, message, messageId: requestId, urgent: params.urgent });
		if (sendResult.isError) {
			return {
				...sendResult,
				details: { ...sendResult.details, op: "request" },
			};
		}
		while (Date.now() < deadline) {
			if (deliveredReply) {
				return {
					content: [{ type: "text", text: formatIncoming(deliveredReply) }],
					details: { op: "request", from: senderId, waited: deliveredReply },
				};
			}
			// Mailbox scan covers FAILED-delivery buffers (peer disposed
			// mid-shutdown, buffered for later drain).
			const reply = bus.takeMatching(senderId, candidate => candidate.replyTo === requestId);
			if (reply) {
				return {
					content: [{ type: "text", text: formatIncoming(reply) }],
					details: { op: "request", from: senderId, waited: reply },
				};
			}
			const { promise: tick, resolve } = Promise.withResolvers<void>();
			setTimeout(resolve, 100);
			await tick;
		}
	} finally {
		unsubscribe();
	}
	return coordinationErrorResult(
		`Request timed out after ${Math.round(timeoutMs / 1000)}s — the peer may still reply later; check wait/inbox.`,
		{ op: "request", from: senderId, to },
	);
}

/** `pid:<pid>:<agentId>` — machine-wide, exact-process addressing. Resolves
 *  the same-id ambiguity when several sessions share (project, Main). */
function parsePidScope(to: string): { pid: number; agentId: string } | null {
	const match = /^pid:(\d+):(.+)$/.exec(to);
	if (!match) return null;
	return { pid: Number(match[1]), agentId: match[2]! };
}

/** System scope: `system:<agentId>` addresses every hub peer machine-wide
 *  regardless of project namespace. `global` is an alias for `system` while
 *  the hub is single-host; once extra transports land (tailnet TCP etc.),
 *  `global` becomes cross-system and `system` stays machine-local. */
function parseSystemScope(to: string): { agentId: string } | null {
	const match = /^(?:system|global):(.+)$/.exec(to);
	if (!match) return null;
	return { agentId: match[1]! };
}
/** `session:<sessionId>` — machine-wide, exact-session addressing. Resolves
 *  the same-id ambiguity when several sessions share (project, Main) without
 *  needing a pid (which changes on restart). Stable across restarts. */
function parseSessionScope(to: string): { sessionId: string; agentId: string } | null {
	// Uniform scope shape: `session:<sessionId>[:<agentId>]`. The agentId
	// suffix is optional (session: always resolves machine-wide Main) but
	// ACCEPTED so the documented `:<peerId>` convention from the other
	// scopes doesn't get swallowed into the sessionId — which produced
	// misleading "no session \"Main\" with sessionId <uuid>:Main" errors.
	const match = /^session:([0-9a-fA-F-]+?)(?::([A-Za-z0-9][A-Za-z0-9_-]*))?$/.exec(to);
	if (!match) return null;
	return { sessionId: match[1]!, agentId: match[2] ?? MAIN_AGENT_ID };
}
/**
 * Project tokens are the broker's presence hashes (wyhash hex of the peer's
 * cwd) — opaque to senders, who instinctively use the cwd basename
 * ("vixie-hq"). Resolve such aliases against the live roster: exact hash
 * match wins; otherwise accept a unique specialism (cwd basename) or a
 * unique project-hash prefix match. An empty return means "no match OR
 * ambiguous" — callers that need to distinguish the two use
 * diagnoseProjectNs.
 */
function resolveProjectNs(
	remotePeers: Array<Pick<HubRosterRow, "project" | "specialism">>,
	token: string,
): string[] {
	if (remotePeers.some(row => row.project === token)) return [token];
	// Specialism: EXACT name wins outright (one unique basename → its
	// project), even when another specialism merely startsWith it
	// ("autoreview" vs "autoreview-two" — suffix disambiguation, not
	// collision). Only when no exact hit exists do we fall through to the
	// hash-prefix family check, where >1 is ambiguous, never a guess.
	const exactSpecialism = remotePeers.filter(
		row => row.specialism !== undefined && row.specialism === token,
	);
	if (exactSpecialism.length === 1) return [...new Set(exactSpecialism.map(row => row.project))];
	const byPrefix = [...new Set(remotePeers.filter(row => row.project.startsWith(token)).map(row => row.project))];
	if (byPrefix.length === 1) return byPrefix;
	return [];
}

/** Visible for tests: project-namespace alias resolution. */
export const resolveProjectNsForTest = resolveProjectNs;

/** Disambiguate a failed resolution for error text: null = unknown token,
 *  array = the colliding candidates (≥2 same-basename cwds, prefix families). */
function diagnoseProjectNs(
	remotePeers: Array<Pick<HubRosterRow, "project" | "specialism">>,
	token: string,
): string[] | null {
	// Mirror of resolveProjectNs: an exact specialism hit means resolution
	// succeeded upstream — never diagnose ambiguity from prefix fuzz
	// ("autoreview" vs "autoreview-two" is suffix disambiguation, not a
	// collision). Only genuine families (≥2 same-basename cwds or a shared
	// hash prefix) are ambiguous.
	const exactSpecialism = remotePeers.filter(
		row => row.specialism !== undefined && row.specialism === token,
	);
	if (exactSpecialism.length === 1) return null;
	const collisions = [
		...new Set([
			...remotePeers.filter(row => row.specialism !== undefined && row.specialism.startsWith(token)).map(row => row.project),
			...remotePeers.filter(row => row.project.startsWith(token)).map(row => row.project),
		]),
	];
	return collisions.length >= 2 ? collisions : null;
}

/** Visible for tests: ambiguity diagnosis for failed alias resolution. */
export const diagnoseProjectNsForTest = diagnoseProjectNs;

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
	urgent?: boolean;
	remotePeers: HubRosterRow[];
	messageId?: string;
	replyTo?: string;
}): Promise<AgentToolResult<CoordinationDetails>> {
	const { senderId, message, scope, remotePeers } = deps;
	const client = currentHubClient();
	// Project tokens are resolved against the live roster by the module-scope
	// resolveProjectNs (hash exact / unique specialism / unique prefix).
	const resolvedNs = client ? resolveProjectNs(remotePeers, scope.project) : [];
	const roster =
		scope.agentId === "all"
			? remotePeers.filter(row => resolvedNs.includes(row.project) && row.agentId !== senderId)
			: remotePeers.filter(row => resolvedNs.includes(row.project) && row.agentId === scope.agentId);
	if (!client || roster.length === 0) {
		const reason = !client
			? "the system-scope hub is not connected"
			: (() => {
					const peers = remotePeers.filter(row => row.agentId === scope.agentId);
					if (peers.length === 0) return `no peer "${scope.agentId}" on the hub roster`;
					const known = [...new Set(peers.map(row => row.project))];
					const ambiguous = diagnoseProjectNs(remotePeers, scope.project);
					const ambiguityNote = ambiguous
						? ` "${scope.project}" is ambiguous — ${ambiguous.length} registered projects match (same-basename checkouts or a shared hash prefix): ${ambiguous.join(", ")}. Use the full hash.`
						: "";
					return (
						`no peer "${scope.agentId}" in project "${scope.project}". ` +
						`Project scopes take the peer's registered namespace (broker presence hash), not a path name.` +
						ambiguityNote +
						` "${scope.agentId}" is registered under: ${known.map(ns => `project:${ns}:${scope.agentId}`).join(", ")}`
					);
				})();
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
		id: deps.messageId ?? `${senderId}-${Date.now()}`,
		ts: Date.now(),
		...(deps.urgent ? { urgent: deps.urgent } : {}),
		...(deps.replyTo ? { replyTo: deps.replyTo } : {}),
	};
	// Single-agent targets MUST carry the resolved hash: the peer's
	// hubTargetMatches compares against its own registered project string, so
	// an unresolved alias ("vixie-hq") would silently never match.
	// Both arms of the old ternary were identical after alias resolution:
	// the roster is already filtered to the resolved namespace, so the
	// single-agent case needs no separate shape. One publish target list.
	const targets = roster.map(row => ({ project: row.project, agentId: row.agentId }));
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

/**
 * Deliver to a machine-wide recipient or broadcast (`system:<id>` /
 * `system:all`): ignores project namespaces entirely — every hub peer
 * matches, project included in the target rows for the broker's own filter.
 */
async function sendSystemScoped(deps: {
	senderId: string;
	message: string;
	agentId: string;
	remotePeers: HubRosterRow[];
	messageId?: string;
	replyTo?: string;
	urgent?: boolean;
}): Promise<AgentToolResult<CoordinationDetails>> {
	const { senderId, message, agentId, remotePeers } = deps;
	const client = currentHubClient();
	const isBroadcast = agentId === "all";
	const roster = isBroadcast
		? remotePeers.filter(row => row.agentId !== senderId)
		: remotePeers.filter(row => row.agentId === agentId);
	if (!client || roster.length === 0) {
		const reason = !client
			? "the system-scope hub is not connected"
			: `no peer "${agentId}" machine-wide`;
		return coordinationErrorResult(`Failed: ${reason}.`, { op: "send", from: senderId, to: `system:${agentId}` });
	}
	const hubMessage = {
		from: senderId,
		to: agentId,
		body: message,
		id: deps.messageId ?? `${senderId}-${Date.now()}`,
		ts: Date.now(),
		...(deps.urgent ? { urgent: deps.urgent } : {}),
		...(deps.replyTo ? { replyTo: deps.replyTo } : {}),
	};
	const targets = roster.map(row => ({ project: row.project, agentId: row.agentId }));
	const result = await client.publish(hubMessage, targets);
	const results =
		result?.results ?? targets.map(entry => ({ to: entry.agentId, ok: false, error: "hub publish failed" }));
	const delivered = results.filter(entry => entry.ok);
	const text =
		isBroadcast
			? `Broadcast delivered to ${delivered.length} of ${targets.length} peer(s) machine-wide.`
			: delivered.length > 0
				? `Delivered to ${agentId} (${targets.length} session${targets.length === 1 ? "" : "s"}).`
				: `Failed: ${results[0]?.error ?? "hub publish failed"}.`;
	return {
		content: [{ type: "text", text }],
		details: { op: "send", from: senderId, to: `system:${agentId}`, receipts: [] },
		isError: delivered.length === 0,
	};
}

/** Exact-process delivery: matches the roster row with this pid. */
async function sendPidScoped(deps: {
	senderId: string;
	message: string;
	pid: number;
	agentId: string;
	remotePeers: HubRosterRow[];
	messageId?: string;
	replyTo?: string;
	urgent?: boolean;
}): Promise<AgentToolResult<CoordinationDetails>> {
	const { senderId, message, pid, agentId, remotePeers } = deps;
	const client = currentHubClient();
	const row = remotePeers.find(candidate => candidate.agentId === agentId && candidate.pid === pid);
	if (!client || !row) {
		const reason = !client ? "the system-scope hub is not connected" : `no session "${agentId}" with pid ${pid}`;
		return coordinationErrorResult(`Failed: ${reason}.`, {
			op: "send",
			from: senderId,
			to: `pid:${pid}:${agentId}`,
		});
	}
	const hubMessage = {
		from: senderId,
		to: agentId,
		body: message,
		id: deps.messageId ?? `${senderId}-${Date.now()}`,
		ts: Date.now(),
		...(deps.urgent ? { urgent: deps.urgent } : {}),
		...(deps.replyTo ? { replyTo: deps.replyTo } : {}),
	};
	const result = await client.publish(hubMessage, [{ project: row.project, agentId, pid }]);
	const ok = result?.results.some(entry => entry.ok) ?? false;
	return {
		content: [{ type: "text", text: ok ? `Delivered to ${agentId} (pid ${pid}).` : `Failed: ${result?.results[0]?.error ?? "hub publish failed"}.` }],
		details: { op: "send", from: senderId, to: `pid:${pid}:${agentId}`, receipts: [] },
		isError: !ok,
	};
}
/** Exact-session delivery: matches the roster row with this sessionId. */
async function sendSessionScoped(deps: {
	senderId: string;
	message: string;
	sessionId: string;
	agentId: string;
	remotePeers: HubRosterRow[];
	messageId?: string;
	replyTo?: string;
	urgent?: boolean;
}): Promise<AgentToolResult<CoordinationDetails>> {
	const { senderId, message, sessionId, agentId, remotePeers } = deps;
	const client = currentHubClient();
	const row = remotePeers.find(candidate => candidate.sessionId === sessionId && candidate.agentId === agentId);
	if (!client || !row) {
		const reason = !client ? "the system-scope hub is not connected" : `no session "${agentId}" with sessionId ${sessionId}`;
		return coordinationErrorResult(`Failed: ${reason}.`, {
			op: "send",
			from: senderId,
			to: `session:${sessionId}`,
		});
	}
	const hubMessage = {
		from: senderId,
		to: agentId,
		body: message,
		id: deps.messageId ?? `${senderId}-${Date.now()}`,
		ts: Date.now(),
		...(deps.replyTo ? { replyTo: deps.replyTo } : {}),
	};
	const result = await client.publish(hubMessage, [{ project: row.project, agentId, sessionId }]);
	const results = result?.results ?? [{ to: agentId, ok: false, error: "hub publish failed" }];
	const delivered = results.filter(entry => entry.ok);
	return {
		content: [{ type: "text", text: delivered.length > 0 ? `Delivered to session ${sessionId}.` : `Failed: ${results[0]?.error ?? "hub publish failed"}.` }],
		details: { op: "send", from: senderId, to: `session:${sessionId}`, receipts: [] },
		isError: delivered.length === 0,
	};
}

/** Namespace qualifier for this process's own project directory. */
export function ownProjectNamespace(): string {
	return hubProjectNamespace(process.cwd());
}

/**
 * Agent-visible identity surface: the caller's own namespace plus the live
 * hub roster (other processes only). This is the ONLY sanctioned way for a
 * session to learn routing identity — project hashes are wyhash hex of the
 * peer's canonical cwd and are NOT derivable from path names, and
 * self-reported identity over IRC conflates under id collisions (every main
 * agent is "Main"). Returns null when the hub is disabled/unreachable.
 */
export async function peerDirectory(): Promise<{
	ownNamespace: string;
	peers: Array<Pick<HubRosterRow, "agentId" | "project" | "status" | "pid" | "sessionId" | "specialism">>;
} | null> {
	// null when the hub is disabled OR disconnected — same signal; callers
	// should surface "hub unavailable" and not loop; {peers: []} means
	// hub is up but no other sessions are present.
	if (!isHubEnabled() || !currentHubClient()) return null;
	const peers = await hubRoster();
	return {
		ownNamespace: ownProjectNamespace(),
		peers: peers.map(row => ({
			agentId: row.agentId,
			project: row.project,
			status: row.status,
			pid: row.pid,
			...(row.sessionId ? { sessionId: row.sessionId } : {}),
			...(row.specialism ? { specialism: row.specialism } : {}),
		})),
	};
}
