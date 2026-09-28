/**
 * System-scope hub broker: a per-user daemon on a 0600 unix socket that
 * relays agent IRC between omp processes on one machine. Spawned lazily by
 * the first connecting client (spawn-on-connect, mirroring the launch
 * broker) and exits after an idle grace with no connected peers.
 *
 * Security posture: the socket is created 0600 under a private parent
 * directory, custom paths whose parent is group/world-writable are refused,
 * and every connection is verified same-uid via SO_PEERCRED (Linux) before
 * any frame is read. There is no TCP transport.
 */
import * as fs from "node:fs";
import * as net from "node:net";
import * as path from "node:path";
import { logger } from "@oh-my-pi/pi-utils";
import type { dlopen as dlopenType, FFIType as ffiTypeType } from "bun:ffi";
import {
	DEFAULT_REQUEST_TIMEOUT_MS,
	encodeFrame,
	FrameStream,
	hubTargetMatches,
	HUB_PROTOCOL_VERSION,
	type HubTarget,
	type HubClientFrame,
	type HubRosterEntry,
	type HubServerFrame,
} from "./protocol";

/** Socket file mode enforced after bind. */
export const HUB_SOCKET_MODE = 0o600;

interface ClientConn {
	socket: net.Socket;
	/** Agent ids this connection registered via hello/status. */
	agents: Set<string>;
	/** Project namespace this connection registered under (from its hello
	 *  identity); empty when unregistered. Bare agentId targets (no explicit
	 *  project) resolve to THIS namespace — the documented HubTarget contract:
	 *  "explicit namespace, or the sender's own when omitted". */
	project: string;
}

export interface HubBrokerOptions {
	socketPath: string;
	idleGraceMs?: number;
	/** Test seam: called once the socket is listening. */
	onListening?: () => void;
	/**
	 * Peer-uid resolver override. Tests inject a mismatching uid to exercise
	 * the cross-uid refusal branch deterministically; production uses the
	 * real SO_PEERCRED implementation.
	 */
	peerUid?: (socket: net.Socket) => number | undefined;
}

/** Resolve the peer uid of a unix socket connection; undefined when unsupported. */
export function peerUid(socket: net.Socket): number | undefined {
	if (process.platform !== "linux") return undefined;
	const handle = (socket as unknown as { _handle?: { fd?: number } })._handle;
	const fd = handle?.fd;
	if (typeof fd !== "number" || fd < 0) return undefined;
	try {
		const ffi = require("bun:ffi") as { dlopen: typeof dlopenType; FFIType: typeof ffiTypeType };
		const { dlopen, FFIType } = ffi;
		const lib = dlopen("libc.so.6", {
			getsockopt: {
				args: [FFIType.i32, FFIType.i32, FFIType.i32, FFIType.ptr, FFIType.ptr],
				returns: FFIType.i32,
			},
		});
		const buf = Bun.allocUnsafe(12);
		const len = Bun.allocUnsafe(4);
		new DataView(len.buffer, len.byteOffset, len.byteLength).setUint32(0, 12, true);
		const rc = lib.symbols.getsockopt(fd, 1 /* SOL_SOCKET */, 17 /* SO_PEERCRED */, buf, len);
		if (rc !== 0) return undefined;
		const view = new DataView(buf.buffer, buf.byteOffset, 12);
		return view.getUint32(4, true);
	} catch {
		return undefined;
	}
}

/** Whether `dir` would let a same-host group/world member tamper with the socket. */
export function parentDirIsUnsafe(dir: string): boolean {
	try {
		const stat = fs.statSync(dir);
		return (stat.mode & 0o022) !== 0;
	} catch {
		return true;
	}
}

/** Project namespace for a cwd: wyhash hex of the canonical path. */
export function hubProjectNamespace(cwd: string): string {
	return Bun.hash.wyhash(path.resolve(cwd)).toString(16).padStart(16, "0");
}

/** Whether a hub broker is already serving `socketPath`. */
function probeLive(socketPath: string): Promise<boolean> {
	// No socket file: guaranteed ENOENT that can fire before any error listener
	// attaches under bun's test runner — fail fast without touching net.
	if (!fs.existsSync(socketPath)) return Promise.resolve(false);
	const { promise, resolve } = Promise.withResolvers<boolean>();
	let socket: net.Socket;
	try {
		socket = net.connect(socketPath);
	} catch {
		return Promise.resolve(false);
	}
	const done = (result: boolean) => {
		socket.destroy();
		resolve(result);
	};
	// Successful connect resolves true — without this a losing broker probing
	// a live winner hangs forever on this promise (zombie process per
	// cold-start storm; eager-subscribe spawns make that N-1 per host boot).
	socket.on("connect", () => done(true));
	socket.on("error", () => done(false));
	return promise;
}

/**
 * Run the broker until the idle grace elapses with zero connected peers.
 * Resolves when the server closes.
 */
export async function startHubBroker(options: HubBrokerOptions): Promise<void> {
	const { socketPath } = options;
	const idleGraceMs = options.idleGraceMs ?? 30_000;

	const parent = path.dirname(socketPath);
	if (parentDirIsUnsafe(parent)) {
		throw new Error(`hub broker: refusing to bind ${socketPath}: parent directory is group/world-writable`);
	}

	// Another live broker? Multiple clients may race to spawn; the winner owns
	// the socket path, the losers exit quietly.
	if (await probeLive(socketPath)) {
		logger.debug("hub broker: socket already served; exiting", { socketPath });
		return;
	}
	try {
		fs.unlinkSync(socketPath);
	} catch (error) {
		if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error;
	}

	const previousUmask = process.umask(0o077);
	const server = net.createServer();
	let pathServer: net.Server | undefined;
	try {
		// Bind on an ABSTRACT namespace first (Linux: no filesystem name, no
		// unlink race), then materialize the filesystem path only once we own
		// the singleton slot. The abstract bind is the true mutual exclusion:
		// a second broker's abstract listen() fails immediately, so the
		// unlink-listen window that let two brokers coexist on one path is gone.
		// Single abstract-namespace bind IS the singleton: one listen() only,
		// under a name derived from the socket path. A second broker's listen
		// on the same abstract name fails immediately — no unlink/rebind window.
		// Clients keep using the filesystem path, which we materialize as a
		// SYMLINK to the abstract name via /proc/net/unix-independent trick:
		// instead, bind the filesystem path on a SECOND server instance only
		// after winning the abstract slot, and relay accepts from it to the
		// same handler set.
		const abstractName = `\0omp-hub-broker:${socketPath}`;
		try {
			server.listen(abstractName);
			const abstractBound = Promise.withResolvers<void>();
			server.once("listening", abstractBound.resolve);
			server.once("error", abstractBound.reject);
			await abstractBound.promise;
		} catch {
			// Abstract name taken → a broker is live (even if its filesystem
			// socket was unlinked mid-race). probeLive said dead, but the race
			// window proves otherwise; stand down.
			logger.debug("hub broker: abstract slot owned; exiting", { socketPath });
			return;
		}
		// Filesystem admission socket on its own server; clients never see the
		// abstract name. Same-uid enforcement and framing are per-connection.
		pathServer = net.createServer();
		try {
			fs.unlinkSync(socketPath);
		} catch (error) {
			if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error;
		}
		const pathListening = Promise.withResolvers<void>();
		pathServer.once("listening", pathListening.resolve);
		pathServer.once("error", pathListening.reject);
		pathServer.listen(socketPath);
		await pathListening.promise;
	} finally {
		process.umask(previousUmask);
	}
	fs.chmodSync(socketPath, HUB_SOCKET_MODE);
	options.onListening?.();
	logger.debug("hub broker: listening", { socketPath });



	const connections = new Set<ClientConn>();
	// Request/reply RPC: requester connections keyed by correlation id, with
	// TTL cleanup. Stateless-per-frame otherwise: the request frame carries
	// everything needed to relay; only replies consult this map.
	const pendingRequests = new Map<string, { conn: ClientConn; timer: NodeJS.Timeout }>();
	// Keyed by project + agentId: agent ids are only unique within a process
	// (every process's main agent is the same constant), so bare keys would let
	// two projects' "main" evict each other from the roster.
	// Per-key SET of registrations: same-key peers coexist (every process's
	// main agent is the same constant, so two sessions in one project share
	// (project, MAIN_AGENT_ID) — replace semantics would evict the first on
	// the second hello and make it permanently invisible via self-exclusion).
	const rosterByAgent = new Map<string, Set<{ entry: HubRosterEntry; conn: ClientConn }>>();

	function rosterKey(project: string, agentId: string): string {
		return `${project}\u0000${agentId}`;
	}
	let idleTimer: NodeJS.Timeout | undefined;

	function armIdleTimer(): void {
		clearTimeout(idleTimer);
		idleTimer = setTimeout(() => {
			logger.debug("hub broker: idle grace elapsed; exiting", { socketPath });
			for (const conn of connections) conn.socket.destroy();
			server.close();
			pathServer?.close(() => {
				try {
					fs.unlinkSync(socketPath);
				} catch {
					// already gone
				}
			});
		}, idleGraceMs);
		idleTimer.unref?.();
	}

	function disarmIdleTimer(): void {
		clearTimeout(idleTimer);
		idleTimer = undefined;
	}

	function rosterSnapshot(exclude?: ClientConn): HubRosterEntry[] {
		const out: HubRosterEntry[] = [];
		for (const records of rosterByAgent.values()) {
			for (const record of records) {
				if (record.conn === exclude) continue;
				out.push(record.entry);
			}
		}
		return out;
	}

	function dropConnection(conn: ClientConn): void {
		if (!connections.delete(conn)) return;
		// Purge this connection's in-flight RPC waits (no socket to route to).
		for (const [id, pending] of pendingRequests) {
			if (pending.conn !== conn) continue;
			clearTimeout(pending.timer);
			pendingRequests.delete(id);
		}
		for (const [key, records] of rosterByAgent) {
			for (const record of records) {
				if (record.conn === conn) records.delete(record);
			}
			if (records.size === 0) rosterByAgent.delete(key);
		}
		if (connections.size === 0) armIdleTimer();
	}

	function send(conn: ClientConn, frame: HubServerFrame): void {
		if (conn.socket.destroyed) return;
		conn.socket.write(encodeFrame(frame) as string);
	}

	function handleFrame(
		conn: ClientConn,
		// `reply` is client→broker here: the answering peer sends it up and the
		// broker routes it back to the requester's connection.
		frame: Exclude<HubClientFrame | HubServerFrame, { type: "error" }>,
	): void {
		switch (frame.type) {
			case "hello": {
				// Protocol version gate: major mismatch is a typed rejection so
				// the client can self-disable instead of misparsing frames.
				if (typeof frame.v === "number" && frame.v !== HUB_PROTOCOL_VERSION) {
					send(conn, {
						type: "error",
						message: `hub protocol version ${frame.v} unsupported (broker speaks ${HUB_PROTOCOL_VERSION})`,
						code: "unsupported-version",
					});
					conn.socket.destroy();
					break;
				}
				for (const entry of frame.agents) {
					if (typeof entry?.agentId !== "string" || !entry.agentId) continue;
					const normalized: HubRosterEntry = {
						agentId: entry.agentId,
						project: String(entry.project ?? ""),
						status: entry.status === "idle" ? "idle" : "running",
						pid: Number(entry.pid) || 0,
						...(entry.sessionFile ? { sessionFile: String(entry.sessionFile) } : {}),
						// Dumb-carrier fields: bounded on receipt so roster size is
						// independent of what clients send.
						...(entry.activity ? { activity: String(entry.activity).slice(0, 120) } : {}),
						...(entry.specialism ? { specialism: String(entry.specialism).slice(0, 120) } : {}),
					};
					conn.agents.add(normalized.agentId);
				conn.project = normalized.project;
					const key = rosterKey(normalized.project, normalized.agentId);
					let records = rosterByAgent.get(key);
					if (!records) {
						records = new Set();
						rosterByAgent.set(key, records);
					}
					// Re-hello from the same conn updates its entry in place.
					for (const existing of records) {
						if (existing.conn === conn) records.delete(existing);
					}
					records.add({ entry: normalized, conn });
				}
				send(conn, {
					type: "welcome",
					v: HUB_PROTOCOL_VERSION,
					self: frame.agents[0]?.agentId ?? "",
					roster: rosterSnapshot(conn),
				});
				break;
			}
			case "status": {
				// Match on the connection's own registration: the status frame
				// carries a bare agentId, which is unique only within the sender.
				// The activity gist rides along (debounced by the sender); the
				// broker overwrites on receipt — no history, no interpretation.
				const nextActivity =
					typeof frame.activity === "string" ? frame.activity.slice(0, 120) : undefined;
				for (const records of rosterByAgent.values()) {
					for (const record of records) {
						if (record.conn === conn && record.entry.agentId === frame.agentId) {
							record.entry.status = frame.status === "idle" ? "idle" : "running";
							if (nextActivity === undefined) delete record.entry.activity;
							else record.entry.activity = nextActivity;
						}
					}
				}
				break;
			}
			case "roster": {
				send(conn, { type: "roster", roster: rosterSnapshot(conn) });
				break;
			}
			case "publish": {
				// Frame-level aggregation: identical target rows (per-roster-row
				// expansion when several sessions share (project, agentId)) must
				// deliver ONE frame per peer connection, not one per target row.
				// Collect deliveries across all targets first, send once per conn,
				// then report per-target success from the shared outcome.
				const deliverConns = new Set<ClientConn>();
				const targetMatches: { agentId: string; conns: Set<ClientConn>; matched: number }[] = [];
				for (const target of frame.targets) {
					if (typeof target?.agentId !== "string" || !target.agentId) continue;

					// Documented HubTarget contract: "explicit namespace, or the
					// sender's own when omitted" — a bare agentId resolves to the
					// PUBLISHING connection's project, never machine-wide fan-out
					// (cross-project noise: every project's Main got every reply).
					const resolvedTarget: HubTarget =
						target.project === undefined ? { ...target, project: conn.project } : target;
					const candidates: { entry: HubRosterEntry; conn: ClientConn }[] = [];
					for (const records of rosterByAgent.values()) {
						for (const record of records) {
							if (record.entry.agentId === target.agentId) candidates.push(record);
						}
					}
					const matches = candidates.filter(candidate => hubTargetMatches(resolvedTarget, candidate.entry));
					const conns = new Set<ClientConn>();
					for (const candidate of matches) {
						if (candidate.conn !== conn) {
							conns.add(candidate.conn);
							deliverConns.add(candidate.conn);
						}
					}
					targetMatches.push({ agentId: target.agentId, conns, matched: matches.length });
				}
				let anyOk = false;
				let lastError: string | undefined;
				for (const targetConn of deliverConns) {
					try {
						send(targetConn, { type: "deliver", msg: { ...frame.msg, urgent: frame.urgent } });
						anyOk = true;
					} catch (error) {
						lastError = error instanceof Error ? error.message : String(error);
					}
				}
				const results = targetMatches.map(({ agentId, conns, matched }) => {
					if (matched === 0) {
						return { to: agentId, ok: false, error: "unknown-agent" as const };
					}
					// Roster proved the peer exists but every holder is the sender's
						// own conn → wrong-namespace style miss, not unknown.
					if (conns.size === 0) {
						return { to: agentId, ok: false, error: "project-mismatch" as const };
					}
					return anyOk
						? ({ to: agentId, ok: true } as const)
						: ({ to: agentId, ok: false, error: lastError } as const);
				});
				send(conn, { type: "publishAck", id: frame.msg?.id ?? "", results });
				break;
			}
			case "request": {
				// Relay like publish (same target matcher), but deliver-style:
				// matching peers receive a `request` frame and answer via
				// `reply`; the broker routes replies back by correlation id.
				// No ack: a miss just times out at the requester.
				const deliverConns = new Set<ClientConn>();
				const targets = "targets" in frame ? frame.targets : [];
				for (const target of targets) {
					if (typeof target?.agentId !== "string" || !target.agentId) continue;
					// Same contract as publish: bare agentId resolves to the
					// requesting connection's project namespace.
					const resolvedTarget: HubTarget =
						target.project === undefined ? { ...target, project: conn.project } : target;
					for (const records of rosterByAgent.values()) {
						for (const record of records) {
							if (record.conn === conn) continue;
							if (record.entry.agentId === target.agentId && hubTargetMatches(resolvedTarget, record.entry)) {
								deliverConns.add(record.conn);
							}
						}
					}
				}
				if (deliverConns.size > 0) {
					const requested = "timeoutMs" in frame ? frame.timeoutMs : undefined;
					const ttl = typeof requested === "number" && requested > 0 ? requested : DEFAULT_REQUEST_TIMEOUT_MS;
					const timer = setTimeout(() => pendingRequests.delete(frame.id), ttl + 1_000);
					timer.unref?.();
					pendingRequests.set(frame.id, { conn, timer });
					// Requester identity for the target's handler: the publishing conn's
					// first registered agentId (agents register per-conn at hello).
					// Without it the target renders an empty sender and cannot
					// address the reply (observed: "hub-peer is not running").
					const fromAgentId = conn.agents.values().next().value ?? "";
					for (const targetConn of deliverConns) {
						try {
							send(targetConn, { type: "request", id: frame.id, msg: frame.msg, from: fromAgentId });
						} catch {
							// dead peer connection: the remaining peers still answer
						}
					}
				}
				break;
			}
			case "reply": {
				// Route back along the requester's connection; first reply
				// wins (the client deletes its waiter, later replies no-op).
				const pending = pendingRequests.get(frame.id);
				if (!pending) break;
				try {
					send(pending.conn, { type: "reply", id: frame.id, from: frame.from, msg: frame.msg });
				} catch {
					// requester gone; entry TTLs out
				}
				break;
			}
			case "ping": {
				send(conn, { type: "pong" });
				break;
			}
			case "bye": {
				send(conn, { type: "bye" });
				conn.socket.end();
				break;
			}
		}
	}

	const resolvePeerUid = options.peerUid ?? peerUid;
	const onConnection = (socket: net.Socket) => {
		const uid = resolvePeerUid(socket);
		if (uid !== undefined && uid !== process.getuid?.()) {
			logger.debug("hub broker: refusing cross-uid connection", { uid, expected: process.getuid?.() });
			socket.destroy();
			return;
		}
		socket.setEncoding("utf8");
		const conn: ClientConn = { socket, agents: new Set(), project: "" };
		const frames = new FrameStream();
		connections.add(conn);
		disarmIdleTimer();

		socket.on("data", chunk => {
			for (const frame of frames.push(chunk)) {
				if (frame.type === "error") {
					logger.debug("hub broker: malformed frame from peer", { socketPath });
					continue;
				}
				handleFrame(conn, frame as Exclude<HubClientFrame | HubServerFrame, { type: "error" }>);
			}
		});
		socket.on("close", () => dropConnection(conn));
		socket.on("error", () => socket.destroy());
	};
	server.on("connection", onConnection);
	if (pathServer) pathServer.on("connection", onConnection);
	server.on("error", error => {
		logger.debug("hub broker: server error", { socketPath, error: String(error) });
	});
	pathServer?.on("error", error => {
		logger.debug("hub broker: path server error", { socketPath, error: String(error) });
	});

	armIdleTimer();
}

/** Entry point for the spawned hub-broker worker (`__omp_worker_hub_broker`). */
export async function startHubBrokerFromEnvironment(): Promise<void> {
	const socketPath = process.env.OMP_HUB_SOCKET_PATH;
	if (!socketPath) throw new Error("hub broker: OMP_HUB_SOCKET_PATH is not set");
	const graceRaw = Number(process.env.OMP_HUB_IDLE_GRACE_MS);
	const idleGraceMs = Number.isFinite(graceRaw) && graceRaw > 0 ? graceRaw : undefined;
	await startHubBroker({ socketPath, idleGraceMs });
}
