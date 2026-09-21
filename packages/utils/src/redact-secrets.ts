/**
 * Secret redaction for any text that reaches a log, transcript, or status line.
 *
 * Covers the credential shapes that leak through error strings: Authorization
 * headers, key-laden URL query params, URL userinfo, and provider-style API
 * keys. Structural URL redaction for log objects stays in
 * `coding-agent/mcp/json-rpc.ts#redactUrlForLog`; this is the text-level barrier.
 */

/** Param names whose URL query values are credentials (single source for URL + text redaction). */
export const SENSITIVE_QUERY_PARAM = /key|token|secret|auth|sig/i;

const REDACTIONS: Array<[RegExp, string]> = [
	// Authorization headers (incl. leaked into error text)
	[/(bearer|basic)\s+[A-Za-z0-9._~+/=-]{8,}/gi, "$1 <redacted>"],
	// Secrets in URL query strings (sensitive param names only)
	[/([?&](?:[^=&\s"']*(?:key|token|secret|auth|sig)[^=&\s"']*)=)[^&\s"']+/gi, "$1<redacted>"],
	// URL userinfo (https://user:pass@host)
	[/(:\/\/)[^/\s:@"']+:[^/\s@"']+@/g, "$1<redacted>@"],
	// Provider-style API keys appearing bare in text
	[/\b(sk|rk)-[A-Za-z0-9-]{16,}\b/g, "<redacted>"],
];

/** Strip credential-shaped substrings from arbitrary text. */
export function redactSecrets(text: string): string {
	let out = text;
	for (const [pattern, replacement] of REDACTIONS) {
		out = out.replace(pattern, replacement);
	}
	return out;
}
