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
import { encodeFrame, FrameStream, type HubClientFrame, type HubRosterEntry, type HubServerFrame } from "./protocol";

/** Socket file mode enforced after bind. */
export const HUB_SOCKET_MODE = 0o600;

interface ClientConn {
	socket: net.Socket;
	/** Agent ids this connection registered via hello/status. */
	agents: Set<string>;
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
	try {
		server.listen(socketPath);
		const listening = Promise.withResolvers<void>();
		server.once("listening", listening.resolve);
		server.once("error", listening.reject);
		await listening.promise;
	} finally {
		process.umask(previousUmask);
	}
	fs.chmodSync(socketPath, HUB_SOCKET_MODE);
	options.onListening?.();
	logger.debug("hub broker: listening", { socketPath });

	const connections = new Set<ClientConn>();
	// Keyed by project + agentId: agent ids are only unique within a process
	// (every process's main agent is the same constant), so bare keys would let
	// two projects' "main" evict each other from the roster.
	const rosterByAgent = new Map<string, { entry: HubRosterEntry; conn: ClientConn }>();

	function rosterKey(project: string, agentId: string): string {
		return `${project}\u0000${agentId}`;
	}
	let idleTimer: NodeJS.Timeout | undefined;

	function armIdleTimer(): void {
		clearTimeout(idleTimer);
		idleTimer = setTimeout(() => {
			logger.debug("hub broker: idle grace elapsed; exiting", { socketPath });
			for (const conn of connections) conn.socket.destroy();
			server.close(() => {
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
		for (const record of rosterByAgent.values()) {
			if (record.conn === exclude) continue;
			out.push(record.entry);
		}
		return out;
	}

	function dropConnection(conn: ClientConn): void {
		if (!connections.delete(conn)) return;
		for (const [agentId, record] of rosterByAgent) {
			if (record.conn === conn) rosterByAgent.delete(agentId);
		}
		if (connections.size === 0) armIdleTimer();
	}

	function send(conn: ClientConn, frame: HubServerFrame): void {
		if (conn.socket.destroyed) return;
		conn.socket.write(encodeFrame(frame) as string);
	}

	function handleFrame(conn: ClientConn, frame: Exclude<HubClientFrame, { type: "error" }>): void {
		switch (frame.type) {
			case "hello": {
				for (const entry of frame.agents) {
					if (typeof entry?.agentId !== "string" || !entry.agentId) continue;
					const normalized: HubRosterEntry = {
						agentId: entry.agentId,
						project: String(entry.project ?? ""),
						status: entry.status === "idle" ? "idle" : "running",
						pid: Number(entry.pid) || 0,
						...(entry.sessionFile ? { sessionFile: String(entry.sessionFile) } : {}),
					};
					conn.agents.add(normalized.agentId);
					rosterByAgent.set(rosterKey(normalized.project, normalized.agentId), { entry: normalized, conn });
				}
				send(conn, { type: "welcome", self: frame.agents[0]?.agentId ?? "", roster: rosterSnapshot(conn) });
				break;
			}
			case "status": {
				// Match on the connection's own registration: the status frame
				// carries a bare agentId, which is unique only within the sender.
				for (const [key, record] of rosterByAgent) {
					if (record.conn === conn && record.entry.agentId === frame.agentId) {
						record.entry.status = frame.status === "idle" ? "idle" : "running";
						void key;
					}
				}
				break;
			}
			case "roster": {
				send(conn, { type: "roster", roster: rosterSnapshot(conn) });
				break;
			}
			case "publish": {
				const results: { to: string; ok: boolean; error?: string }[] = [];
				for (const target of frame.targets) {
					if (typeof target?.agentId !== "string" || !target.agentId) continue;
					// A target without a project means the SENDER's namespace: the
					// client resolves that before publishing, so an unqualified
					// target here matches any single registration of the id only
					// when it is unambiguous across the roster.
					const candidates = [...rosterByAgent.values()].filter(
						candidate => candidate.entry.agentId === target.agentId,
					);
					const record =
						target.project !== undefined
							? candidates.find(candidate => candidate.entry.project === target.project)
							: candidates.length === 1
								? candidates[0]
								: undefined;
					if (record === undefined || record.conn === conn) {
						results.push({
							to: target.agentId,
							ok: false,
							// Known id but wrong namespace → project-mismatch; the
							// roster proves the peer exists, just not here.
							error: candidates.length > 0 ? "project-mismatch" : "unknown-agent",
						});
						continue;
					}
					try {
						send(record.conn, { type: "deliver", msg: frame.msg });
						results.push({ to: target.agentId, ok: true });
					} catch (error) {
						results.push({
							to: target.agentId,
							ok: false,
							error: error instanceof Error ? error.message : String(error),
						});
					}
				}
				send(conn, { type: "publishAck", id: frame.msg?.id ?? "", results });
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
	server.on("connection", socket => {
		const uid = resolvePeerUid(socket);
		if (uid !== undefined && uid !== process.getuid?.()) {
			logger.debug("hub broker: refusing cross-uid connection", { uid, expected: process.getuid?.() });
			socket.destroy();
			return;
		}
		socket.setEncoding("utf8");
		const conn: ClientConn = { socket, agents: new Set() };
		const frames = new FrameStream();
		connections.add(conn);
		disarmIdleTimer();

		socket.on("data", chunk => {
			for (const frame of frames.push(chunk)) {
				if (frame.type === "error") {
					logger.debug("hub broker: malformed frame from peer", { socketPath });
					continue;
				}
				handleFrame(conn, frame as Exclude<HubClientFrame, { type: "error" }>);
			}
		});
		socket.on("close", () => dropConnection(conn));
		socket.on("error", () => socket.destroy());
	});

	server.on("error", error => {
		logger.debug("hub broker: server error", { socketPath, error: String(error) });
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
