// Live verification: hub RPC blocks until the agent's actual turn reply.
import { HubClient } from "../../src/irc/remote/client";

async function main() {
	const client = await HubClient.connect({
		socketPath: "/home/dave/.omp/agent/hub.sock",
		identity: { agentId: "verifier", project: "rpc-verify", status: "running" as const, pid: process.pid },
	});
	if (!client) { console.error("FAIL: no hub client"); process.exit(1); }
	console.log("connected ✓");
	const roster = await client.roster();
	console.log(`roster: ${roster.length}:`, roster.map(r => `${r.agentId}/${r.pid}`).join(" "));
	const target = roster.find(r => r.status === "running") ?? roster[0];
	if (!target) { console.error("no target agent"); process.exit(1); }
	console.log(`firing RPC at ${target.agentId} pid=${target.pid} (120s timeout) ...`);
	const t0 = Date.now();
	const reply = await client.request(
		{ type: "text", text: "Reply with exactly: HUB_RPC_OK" } as never,
		[{ agentId: target.agentId, pid: target.pid }] as never,
		120_000,
	);
	const dt = ((Date.now() - t0) / 1000).toFixed(1);
	console.log(`reply after ${dt}s:`, JSON.stringify(reply, null, 2));
	client.close();
}
main();
