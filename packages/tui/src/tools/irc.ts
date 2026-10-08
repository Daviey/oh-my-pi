/** Peer-message types and roster constants shared by messaging, `wait`, and child prompts. */

/** Maximum live peer rows embedded in child prompts. */
export const DEFAULT_PEER_ROSTER_LIMIT = 32;

/** Serializable peer message retained in coordination result snapshots. */
export interface IrcMessage {
	id: string;
	/** Sender agent id. */
	from: string;
	/** Recipient agent id (resolved; "all" is expanded by the tool, not stored). */
	to: string;
	body: string;
	ts: number;
	/** Message id being answered. */
	replyTo?: string;
	/**
	 * Sender's session id, stamped by the hub transports at transmit. Agent
	 * ids collide across processes (every main agent is "Main"), so this is
	 * the unambiguous reply address: `session:<id>` targets it exactly,
	 * cross-project included. Absent on messages from pre-session peers.
	 */
	fromSessionId?: string;
	/**
	 * Automated wake-turn relay of a woken subagent's stop output (task executor
	 * `relayWakeTurnOutput`). Relays are answers, never wake sources: the
	 * recipient's own wake-turn relay must skip them or two idle peers
	 * ping-pong forever.
	 */
	wakeRelay?: boolean;
	/**
	 * High-importance delivery. Mid-turn the message routes through the
	 * steering queue (interrupting foreground tools at the next steering
	 * poll, ~250ms) instead of the interrupt queue, which only surfaces at
	 * step boundaries and can starve behind one long tool call or model
	 * response indefinitely.
	 */
	urgent?: boolean;
}

/** Delivery outcome for one peer recipient. */
export interface IrcDeliveryReceipt {
	to: string;
	outcome: "injected" | "woken" | "revived" | "failed";
	error?: string;
}
/** Status ordering for peer rosters in child prompts. */
export const LIST_STATUS_ORDER: Record<string, number> = { running: 0, idle: 1, parked: 2 };
