import { describe, expect, it } from "bun:test";
import { redactSecrets } from "@oh-my-pi/pi-utils/redact-secrets";

describe("redactSecrets", () => {
	it("strips bearer tokens from error text", () => {
		const out = redactSecrets("request died: Authorization: Bearer abcdef1234567890abcdef");
		expect(out).not.toContain("abcdef1234567890");
		expect(out).toContain("Bearer <redacted>");
	});

	it("redacts credential-bearing URL query params, keeps benign ones", () => {
		const out = redactSecrets("failed https://api.example.test/mcp?apiKey=SECRET123&limit=10");
		expect(out).not.toContain("SECRET123");
		expect(out).toContain("apiKey=<redacted>");
		expect(out).toContain("limit=10");
	});

	it("redacts URL userinfo", () => {
		const out = redactSecrets("connect https://admin:hunter2@example.test/feed");
		expect(out).not.toContain("hunter2");
		expect(out).toContain("https://<redacted>@example.test/feed");
	});

	it("redacts bare provider-style keys", () => {
		const out = redactSecrets("key sk-abcdefghijklmnop-123456 expired");
		expect(out).not.toContain("sk-abcdefghijklmnop");
		expect(out).toContain("<redacted>");
	});

	it("leaves ordinary prose and hex untouched", () => {
		const text = "token estimates done; backup saved 7c9f01d4e2a63b8f5c9014de77a2b6e0";
		expect(redactSecrets(text)).toBe(text);
	});
});
