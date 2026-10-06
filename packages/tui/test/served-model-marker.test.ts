import { describe, expect, it } from "bun:test";
import type { AssistantMessage } from "@oh-my-pi/pi-ai/types";
import { detectServedModelMismatch, ServedModelTracker } from "@oh-my-pi/pi-tui/chat/served-model-marker";

function turn(parts: {
	model: string;
	served?: string;
	provider?: string;
	upstreamProvider?: string;
	routingReport?: {
		requested: string;
		route: string;
		reason: string;
		method: string;
		failovers: { provider: string; reason: string }[];
	};
}): AssistantMessage {
	return {
		role: "assistant",
		content: [],
		api: "anthropic-messages",
		provider: parts.provider ?? "openrouter",
		model: parts.model,
		...(parts.served ? { upstreamModel: parts.served } : {}),
		...(parts.upstreamProvider ? { upstreamProvider: parts.upstreamProvider } : {}),
		...(parts.routingReport ? { routingReport: parts.routingReport } : {}),
		usage: {
			input: 0,
			output: 0,
			cacheRead: 0,
			cacheWrite: 0,
			totalTokens: 0,
			cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 },
		},
		stopReason: "stop",
		timestamp: 0,
	};
}

describe("detectServedModelMismatch", () => {
	it("flags a different family served under the requested id, naming the route", () => {
		expect(
			detectServedModelMismatch(
				turn({
					model: "anthropic/claude-opus-5",
					served: "claude-haiku-4-5-20251001",
					upstreamProvider: "Amazon Bedrock",
				}),
			),
		).toEqual({
			requested: "anthropic/claude-opus-5",
			served: "claude-haiku-4-5-20251001",
			provider: "openrouter",
			upstreamProvider: "Amazon Bedrock",
		});
	});

	it("flags a different revision of the same family", () => {
		expect(detectServedModelMismatch(turn({ model: "claude-opus-4-6", served: "claude-opus-4-7" }))?.served).toBe(
			"claude-opus-4-7",
		);
	});

	it("treats a dated snapshot or gateway prefix of the requested model as the same model", () => {
		expect(
			detectServedModelMismatch(turn({ model: "anthropic/claude-haiku-4.5", served: "claude-haiku-4-5-20251001" })),
		).toBeUndefined();
	});

	it("treats an unclassifiable served id (first-party A/B codename) as unverifiable, not a substitution", () => {
		expect(
			detectServedModelMismatch(
				turn({ model: "claude-opus-4-6", served: "numbat-v6-efforts-20-40-80-ab-prod", provider: "anthropic" }),
			),
		).toBeUndefined();
	});

	it("stays silent when no served id was recovered", () => {
		expect(detectServedModelMismatch(turn({ model: "claude-fable-5-1" }))).toBeUndefined();
	});
	it("flags a route-alias request served by a known model (router substitution)", () => {
		// Requested ids a router resolves to concrete models ("default", "smol")
		// classify as unknown; that must not suppress the marker when the served
		// model IS classifiable — the exact coxswain case.
		const mismatch = detectServedModelMismatch(turn({ model: "default", served: "glm-5.3" }));
		expect(mismatch).toBeDefined();
		expect(mismatch?.requested).toBe("default");
		expect(mismatch?.served).toBe("glm-5.3");
	});

	it("stays silent for route-alias requests with no recoverable served model", () => {
		// Unverifiable served id + unknown requested: no evidence of substitution.
		expect(
			detectServedModelMismatch(turn({ model: "smol", served: "numbat-v6-efforts-20-40-80-ab-prod" })),
		).toBeUndefined();
	});

	it("carries the router routing report into the mismatch", () => {
		const report = {
			requested: "default",
			route: "default",
			reason: "as-requested",
			method: "default",
			failovers: [{ provider: "mockA", reason: "quota" }],
		};
		const mismatch = detectServedModelMismatch(turn({ model: "default", served: "glm-5.3", routingReport: report }));
		expect(mismatch?.routingReport).toEqual(report);
		expect(mismatch?.routingReport?.failovers[0].provider).toBe("mockA");
	});
});

describe("ServedModelTracker", () => {
	it("reports each substitution pair once, and a new pair after a model switch", () => {
		const tracker = new ServedModelTracker();
		const swapped = turn({ model: "claude-opus-5", served: "claude-haiku-4-5" });
		expect(tracker.check(swapped)).toBeDefined();
		expect(tracker.check(swapped)).toBeUndefined();
		expect(tracker.check(turn({ model: "claude-opus-5", served: "claude-opus-5" }))).toBeUndefined();
		expect(tracker.check(turn({ model: "claude-sonnet-5", served: "claude-haiku-4-5" }))?.requested).toBe(
			"claude-sonnet-5",
		);
		expect(tracker.check(swapped)).toBeUndefined();
	});
});
