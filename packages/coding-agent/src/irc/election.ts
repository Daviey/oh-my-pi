/**
 * Frame-layer leader election for the system-scope hub. Sessions
 * self-organize into leader / middle / member on BOTH transports: claims
 * and heartbeats ride the broadcast paths (unix broker fan-out; MQTT
 * frames topic), so nothing above this module addresses them per-peer.
 *
 * Leases, not locks: a claim is valid only while `leaseUntil` is in the
 * future. A node claims when it sees no valid lease (or its own); the
 * loser of a contention race defers and settles into "middle" (a
 * leader-assigned relay slot) or plain "member". Incumbents refresh
 * before expiry, so steady state is one leader per area.
 *
 * Ordering: earliest ESTABLISHMENT wins (`claimedAt`), not latest
 * deadline — deadline comparison would let every late joiner steal
 * leadership with a freshly-further lease. Clock ties break on the
 * session id string, so both contenders reach the same verdict.
 */
import type { HubClientLike } from "./remote/client";
import type { HubElectionServerFrame } from "./remote/protocol";

/** Self-organized role. "member" is the default for every node that has
 *  not (yet) won a claim — including agents that never claim. */
export type ElectionRole = "leader" | "middle" | "member";

/** The lease facts every claim carries (wire subset of leaderClaim). */
export interface ElectionLease {
	leaderSessionId: string;
	leaseUntil: number;
	claimedAt: number;
}

/** Election tunables; compact for tests (short TTLs instead of fake
 *  timers — the unix broker keeps its own Node timers, so mocked clocks
 *  cannot drive cross-process frames). */
export interface ElectionOptions {
	/** Claim lease TTL (ms). Default 60s. */
	leaseTtlMs?: number;
	/** Incumbent refresh interval (ms). Default 20s. */
	refreshMs?: number;
	/** Heartbeat interval (ms). Default 30s. */
	heartbeatMs?: number;
	/** Max relay slots a leader assigns. Default 2. */
	maxMiddles?: number;
	/** Delay before a node's first claim (ms). Default 50ms. */
	claimDelayMs?: number;
	/** Clock for lease math (test seam). */
	now?: () => number;
	/** Scheduler (test seam). */
	schedule?: (fn: () => void, ms: number) => { clear: () => void };
}

/** Contention verdict: does the incoming claim replace the local lease?
 *  Same leader always wins (refresh); otherwise earliest establishment
 *  wins, with the session id as the deterministic clock-tie break. */
export function claimWins(incoming: ElectionLease, local: ElectionLease | undefined, now: number): boolean {
	if (!local || local.leaseUntil < now) return true;
	if (incoming.leaderSessionId === local.leaderSessionId) return true;
	if (incoming.claimedAt !== local.claimedAt) return incoming.claimedAt < local.claimedAt;
	return incoming.leaderSessionId < local.leaderSessionId;
}

/** One node's election state machine. Attach one per process; the hub
 *  manager wires it to the live client and folds roles into the roster. */
export class ElectionNode {
	readonly ownSessionId: string;
	#role: ElectionRole = "member";
	#lease: ElectionLease | undefined;
	/** Election telemetry (since attach): split-brain post-mortems without
	 *  raw-log archaeology. Counters only — derived, never load-bearing. */
	#stats = { claimsSeen: 0, claimsRejected: 0, abdicationsSeen: 0, roleFlips: 0 };
	// Clock-skew anchor: the transport stamps authoritative receive time
	// (brokerTs on unix fan-out; sentTs on MQTT) onto election frames. Once
	// seen, expiry math runs in the STAMPING clock plus local monotonic
	// elapsed — immune to this host's wall-clock drift vs the leader's.
	#clockAnchor: { stampTs: number; monoAt: number } | undefined;
	#middles = new Set<string>();
	#knownPeers: string[] = [];
	#client: HubClientLike | undefined;
	#timers: { clear: () => void }[] = [];
	#closed = false;
	readonly options: Required<Pick<ElectionOptions, "leaseTtlMs" | "refreshMs" | "heartbeatMs" | "maxMiddles" | "claimDelayMs">> & {
		now: () => number;
		schedule: (fn: () => void, ms: number) => { clear: () => void };
	};

	constructor(ownSessionId: string, options: ElectionOptions = {}) {
		this.ownSessionId = ownSessionId;
		this.options = {
			leaseTtlMs: options.leaseTtlMs ?? 60_000,
			refreshMs: options.refreshMs ?? 20_000,
			heartbeatMs: options.heartbeatMs ?? 30_000,
			maxMiddles: options.maxMiddles ?? 2,
			claimDelayMs: options.claimDelayMs ?? 50,
			now: options.now ?? (() => Date.now()),
			schedule: options.schedule ?? ((fn, ms) => {
				const timer = setTimeout(fn, ms);
				timer.unref?.();
				return { clear: () => clearTimeout(timer) };
			}),
		};
	}

	/** Skew-aware clock: anchor clock when a stamped frame arrived, else
	 *  the injected seam (tests) / wall clock. */
	#now(): number {
		if (this.#clockAnchor) {
			return this.#clockAnchor.stampTs + (this.#monoNow() - this.#clockAnchor.monoAt);
		}
		return this.options.now();
	}

	#monoNow(): number {
		return performance.now();
	}

	/** Telemetry counters (copy — callers can hold it). */
	electionStats(): { claimsSeen: number; claimsRejected: number; abdicationsSeen: number; roleFlips: number } {
		return { ...this.#stats };
	}

	/** Role transitions counted here; call at every assignment site. */
	#setRole(role: ElectionRole): void {
		if (role !== this.#role) this.#stats.roleFlips++;
		this.#role = role;
	}

	/** Current self-organized role. A leader whose lease lapsed without a
	 *  successful refresh reports the truth ("member"), not the hope. */
	electionRole(): ElectionRole {
		if (this.#role === "leader" && this.#lease && this.#lease.leaseUntil < this.#now()) {
			return "member";
		}
		return this.#role;
	}

	/** Live lease snapshot (roster-role annotation; defensive copy). */
	currentLease(): ElectionLease | undefined {
		return this.#lease ? { ...this.#lease } : undefined;
	}

	/** Current relay-slot assignment from the leader (defensive copy). */
	currentMiddles(): string[] {
		return [...this.#middles];
	}

	/** Role this node's lease facts assign to an arbitrary session id —
	 *  how the hub manager annotates roster rows it did not register. */
	roleFor(sessionId: string | undefined): ElectionRole | undefined {
		const lease = this.#lease;
		if (!lease || !sessionId) return undefined;
		if (sessionId === lease.leaderSessionId) return "leader";
		if (this.#middles.has(sessionId)) return "middle";
		return "member";
	}

	/** Leader-side observation hook: the hub manager feeds peer session
	 *  ids from the merged roster so slot assignment tracks reality. */
	observePeers(sessionIds: string[]): void {
		this.#knownPeers = [...new Set(sessionIds)].filter(id => id && id !== this.ownSessionId);
	}

	/**
	 * Wire this node to a hub client: claim after the delay, refresh while
	 * leading, heartbeat throughout, and receive the election frames the
	 * client routes in. Idempotent — re-attaching resets the loops.
	 */
	attach(client: HubClientLike): void {
		this.close();
		this.#closed = false;
		this.#client = client;
		client.onElection?.(frame => this.#receive(frame));
		this.#arm(() => this.#beat(), 0);
		this.#arm(() => this.#evaluate(), this.options.claimDelayMs);
	}

	/** Stop every loop and detach from the client (shutdown/test seam). */
	close(): void {
		// Graceful abdication: if we hold the lease, tell the fleet before
		// tearing down so followers contest after claimDelayMs instead of
		// waiting out leaseTtlMs leaderless. Fire-and-forget by design.
		if (!this.#closed && this.#role === "leader" && this.#client) {
			try {
				this.#client.sendElection({ type: "leaderAbdicate", leaderSessionId: this.ownSessionId });
			} catch {
				// transport already dying — lease expiry is the backstop
			}
		}
		this.#closed = true;
		for (const timer of this.#timers) timer.clear();
		this.#timers = [];
		this.#client?.onElection?.(null);
		this.#client = undefined;
	}

	#arm(fn: () => void, ms: number): void {
		const timer = this.options.schedule(() => {
			this.#timers = this.#timers.filter(entry => entry !== timer);
			fn();
		}, ms);
		this.#timers.push(timer);
	}

	/** Fold one routed election frame into local state. Heartbeats carry
	 *  presence only (the transports merge them into roster rows); the
	 *  state machine proper reacts to claims alone. */
	#receive(frame: HubElectionServerFrame): void {
		if (this.#closed) return;
		if (frame.type === "leaderAbdicate") {
			// Only the current lease holder may open the seat early;
			// strangers' abdications are ignored.
			if (!this.#lease || this.#lease.leaderSessionId !== frame.leaderSessionId) return;
			if (this.#lease.leaderSessionId === this.ownSessionId) return; // own echo
			this.#lease = undefined;
			this.#stats.abdicationsSeen++;
			this.#setRole("member");
			// Contest the open seat after the claim delay — contention
			// resolves via claimWins (earliest claimedAt, id tiebreak).
			this.#arm(() => this.#evaluate(), this.options.claimDelayMs);
			return;
		}
		if (frame.type !== "leaderClaim") return;
		if (typeof frame.leaderSessionId !== "string" || !frame.leaderSessionId) return;
		if (typeof frame.leaseUntil !== "number" || !Number.isFinite(frame.leaseUntil)) return;
		if (typeof frame.claimedAt !== "number" || !Number.isFinite(frame.claimedAt)) return;
		const stamp = (frame as { brokerTs?: number; sentTs?: number }).brokerTs ?? (frame as { sentTs?: number }).sentTs;
		if (typeof stamp === "number" && Number.isFinite(stamp)) {
			this.#clockAnchor = { stampTs: stamp, monoAt: this.#monoNow() };
		}
		const incoming: ElectionLease = {
			leaderSessionId: frame.leaderSessionId,
			leaseUntil: frame.leaseUntil,
			claimedAt: frame.claimedAt,
		};
		// Self-echo (MQTT loopback of our own broadcast, broker reflect):
		// adopt it as confirmation only while we still believe we lead.
		if (incoming.leaderSessionId === this.ownSessionId) {
			if (this.#role === "leader") {
				this.#lease = incoming;
				this.#applyMiddles(frame.middles);
			}
			return;
		}
		this.#stats.claimsSeen++;
		if (!claimWins(incoming, this.#lease, this.#now())) {
			this.#stats.claimsRejected++;
			return;
		}
		this.#lease = incoming;
		this.#applyMiddles(frame.middles);
		// A valid foreign lease means we are not the leader — settle into
		// whatever slot it assigned us.
		this.#setRole(this.#middles.has(this.ownSessionId) ? "middle" : "member");
		this.#arm(() => this.#evaluate(), Math.max(1, incoming.leaseUntil - this.#now() + 1));
	}

	/** Leader-side: replace the relay-slot assignment (bounded against
	 *  garbage frames from peers we cannot trust to be well-formed). */
	#applyMiddles(middles: string[] | undefined): void {
		this.#middles = new Set(
			(middles ?? []).filter(id => typeof id === "string" && id.length > 0).slice(0, this.options.maxMiddles * 4),
		);
	}

	/** Periodic driver: refresh while leading with a valid lease, claim
	 *  when there is no valid lease, otherwise wait this one out. */
	#evaluate(): void {
		if (this.#closed || !this.#client) return;
		const now = this.#now();
		const lease = this.#lease;
		if (lease && lease.leaderSessionId === this.ownSessionId && lease.leaseUntil >= now) {
			// Incumbent refresh: same establishment time, extended deadline.
			const middles = this.#leaderMiddles();
			this.#lease = { ...lease, leaseUntil: now + this.options.leaseTtlMs };
			this.#sendClaim(this.#lease, middles);
			this.#arm(() => this.#evaluate(), this.options.refreshMs);
			return;
		}
		if (!lease || lease.leaseUntil < now) {
			// Open seat: claim it. Optimistic leader until a better claim
			// arrives — contention is settled by claimWins on receipt.
			const middles = this.#leaderMiddles();
			this.#lease = { leaderSessionId: this.ownSessionId, leaseUntil: now + this.options.leaseTtlMs, claimedAt: now };
			this.#setRole("leader");
			this.#sendClaim(this.#lease, middles);
			this.#arm(() => this.#evaluate(), this.options.refreshMs);
			return;
		}
		// Valid foreign lease: re-check when it expires.
		this.#arm(() => this.#evaluate(), Math.max(1, lease.leaseUntil - now + 1));
	}

	/** Relay-slot computation: first `maxMiddles` observed peers. */
	#leaderMiddles(): string[] {
		if (this.#role !== "leader") return [];
		return this.#knownPeers.slice(0, this.options.maxMiddles);
	}

	/** Presence heartbeat loop; runs for every role. */
	#beat(): void {
		if (this.#closed || !this.#client) return;
		this.#client.sendElection?.({ type: "heartbeat", lastSeen: this.options.now() });
		this.#arm(() => this.#beat(), this.options.heartbeatMs);
	}

	/** Broadcast one leader claim over the transport's election path. */
	#sendClaim(lease: ElectionLease, middles: string[]): void {
		this.#client?.sendElection?.({
			type: "leaderClaim",
			leaderSessionId: lease.leaderSessionId,
			leaseUntil: lease.leaseUntil,
			claimedAt: lease.claimedAt,
			middles,
		});
	}
}
