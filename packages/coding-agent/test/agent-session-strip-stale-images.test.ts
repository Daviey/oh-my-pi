import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import * as path from "node:path";
import { Agent } from "@oh-my-pi/pi-agent-core";
import { getBundledModel } from "@oh-my-pi/pi-catalog/models";
import { ModelRegistry } from "@oh-my-pi/pi-coding-agent/config/model-registry";
import { Settings } from "@oh-my-pi/pi-coding-agent/config/settings";
import { AgentSession } from "@oh-my-pi/pi-coding-agent/session/agent-session";
import { AuthStorage } from "@oh-my-pi/pi-coding-agent/session/auth-storage";
import { SessionManager } from "@oh-my-pi/pi-coding-agent/session/session-manager";
import { TempDir } from "@oh-my-pi/pi-utils";

/**
 * Pins the per-turn auto recency-strip (compaction.stripStaleImages): after a
 * turn completes, image blocks older than the newest image-bearing turn are
 * stripped from BOTH the live context and the persisted session file, while
 * the newest screenshot stays pixel-true.
 */
describe("AgentSession auto strip-stale-images", () => {
	let tempDir: TempDir;
	let session: AgentSession;
	let sessionManager: SessionManager;
	let authStorage: AuthStorage;

	const PNG_OLD: ImageContent = { type: "image", data: "iVBORw0KGgo", mimeType: "image/png" };
	const PNG_NEW: ImageContent = { type: "image", data: "iVBORw0KGg=", mimeType: "image/png" };

	beforeEach(async () => {
		tempDir = TempDir.createSync("@pi-strip-stale-images-");
		authStorage = await AuthStorage.create(path.join(tempDir.path(), "testauth.db"));
		authStorage.keys.setRuntime("anthropic", "test-key");
		const modelRegistry = new ModelRegistry(authStorage);
		sessionManager = SessionManager.create(tempDir.path(), tempDir.path());

		const bundled = getBundledModel("anthropic", "claude-sonnet-4-5");
		if (!bundled) throw new Error("Expected built-in anthropic model to exist");
		const model = { ...bundled, contextWindow: 200_000, maxTokens: 64_000 };

		const now = Date.now();
		const usageZero = {
			input: 0,
			output: 0,
			cacheRead: 0,
			cacheWrite: 0,
			totalTokens: 0,
			cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 },
		};
		sessionManager.appendMessage({
			role: "user",
			content: [{ type: "text", text: "old screenshot" }, PNG_OLD],
			timestamp: now - 100,
		});
		sessionManager.appendMessage({
			role: "assistant",
			content: [{ type: "text", text: "That screenshot shows the login page." }],
			api: "anthropic-messages",
			provider: "anthropic",
			model: "claude-sonnet-4-5",
			stopReason: "stop",
			usage: usageZero,
			timestamp: now - 90,
		});
		sessionManager.appendMessage({
			role: "user",
			content: [{ type: "text", text: "new screenshot" }, PNG_NEW],
			timestamp: now - 50,
		});
		sessionManager.appendMessage({
			role: "assistant",
			content: [{ type: "text", text: "This one shows the settings page." }],
			api: "anthropic-messages",
			provider: "anthropic",
			model: "claude-sonnet-4-5",
			stopReason: "stop",
			usage: usageZero,
			timestamp: now - 40,
		});

		const agent = new Agent({
			initialState: { model, systemPrompt: ["Test"], tools: [], messages: [] },
		});
		session = new AgentSession({
			agent,
			sessionManager,
			settings: Settings.isolated({
				"compaction.enabled": false,
				"compaction.stripStaleImages": true,
			}),
			modelRegistry,
		});
		session.agent.replaceMessages(session.buildDisplaySessionContext().messages);
	});

	afterEach(async () => {
		try {
			await session?.dispose();
		} finally {
			authStorage?.close();
			await tempDir?.remove();
		}
	});

	function userImages(messages: Array<{ role: string; content: unknown }>): number[] {
		return messages
			.filter(m => m.role === "user" && Array.isArray(m.content))
			.map(m => (m.content as Array<{ type: string }>).filter(b => b.type === "image").length);
	}

	function turnEnds(): void {
		const finalAssistant = {
			role: "assistant" as const,
			content: [{ type: "text" as const, text: "Continuing." }],
			api: "anthropic-messages" as const,
			provider: "anthropic",
			model: "claude-sonnet-4-5",
			stopReason: "stop" as const,
			usage: {
				input: 100,
				output: 10,
				cacheRead: 0,
				cacheWrite: 0,
				totalTokens: 110,
				cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 },
			},
			timestamp: Date.now(),
		};
		session.agent.emitExternalEvent({ type: "message_end", message: finalAssistant });
		session.agent.emitExternalEvent({ type: "agent_end", messages: [finalAssistant] });
	}

	it("strips older screenshots on turn end, keeping the newest pixel-true (live + persisted)", async () => {
		turnEnds();
		await session.waitForIdle();

		const live = session.agent.state.messages as Array<{ role: string; content: unknown }>;
		expect(userImages(live)).toEqual([0, 1]);

		await sessionManager.flush();
		const sessionFile = sessionManager.getSessionFile();
		if (!sessionFile) throw new Error("Expected a persisted session file");
		const reloaded = await SessionManager.open(sessionFile, tempDir.path());
		const rebuilt = reloaded.buildSessionContext().messages as Array<{ role: string; content: unknown }>;
		expect(userImages(rebuilt)).toEqual([0, 1]);
	});

	it("is a no-op when only one image turn exists (nothing stale)", async () => {
		// Drop the old-image message by rebuilding the fixture: easiest is a fresh session.
		await session.dispose();
		authStorage.close();
		await tempDir.remove();

		tempDir = TempDir.createSync("@pi-strip-stale-images-");
		authStorage = await AuthStorage.create(path.join(tempDir.path(), "testauth.db"));
		authStorage.keys.setRuntime("anthropic", "test-key");
		const modelRegistry = new ModelRegistry(authStorage);
		sessionManager = SessionManager.create(tempDir.path(), tempDir.path());
		const bundled = getBundledModel("anthropic", "claude-sonnet-4-5");
		if (!bundled) throw new Error("Expected built-in anthropic model to exist");
		const agent = new Agent({
			initialState: { model: bundled, systemPrompt: ["Test"], tools: [], messages: [] },
		});
		session = new AgentSession({
			agent,
			sessionManager,
			settings: Settings.isolated({
				"compaction.enabled": false,
				"compaction.stripStaleImages": true,
			}),
			modelRegistry,
		});
		const now = Date.now();
		sessionManager.appendMessage({
			role: "user",
			content: [{ type: "text", text: "only screenshot" }, PNG_NEW],
			timestamp: now,
		});
		session.agent.replaceMessages(session.buildDisplaySessionContext().messages);

		turnEnds();
		await session.waitForIdle();

		const live = session.agent.state.messages as Array<{ role: string; content: unknown }>;
		expect(userImages(live)).toEqual([1]);
	});

	it("is a no-op when compaction.stripStaleImages is false", async () => {
		// Drop the fixture session/manager to rebuild with different settings
		await session.dispose();
		authStorage.close();
		await tempDir.remove();

		tempDir = TempDir.createSync("@pi-strip-stale-images-off-");
		authStorage = await AuthStorage.create(path.join(tempDir.path(), "testauth.db"));
		const modelRegistry = new ModelRegistry(authStorage);
		sessionManager = SessionManager.create(tempDir.path(), tempDir.path());
		const bundled = getBundledModel("anthropic", "claude-sonnet-4-5");
		if (!bundled) throw new Error("Expected built-in anthropic model to exist");
		const agent = new Agent({
			initialState: { model: bundled, systemPrompt: ["Test"], tools: [], messages: [] },
		});
		session = new AgentSession({
			agent,
			sessionManager,
			settings: Settings.isolated({
				"compaction.enabled": false,
				"compaction.stripStaleImages": false,
			}),
			modelRegistry,
		});
		sessionManager.appendMessage({
			role: "user",
			content: [{ type: "text", text: "old" }, PNG_OLD],
			timestamp: Date.now() - 100,
		});
		sessionManager.appendMessage({
			role: "user",
			content: [{ type: "text", text: "new" }, PNG_NEW],
			timestamp: Date.now() - 50,
		});
		session.agent.replaceMessages(session.buildDisplaySessionContext().messages);

		turnEnds();
		await session.waitForIdle();

		const live = session.agent.state.messages as Array<{ role: string; content: unknown }>;
		expect(userImages(live)).toEqual([1, 1]);
	});
});

type ImageContent = { type: "image"; data: string; mimeType: string };
