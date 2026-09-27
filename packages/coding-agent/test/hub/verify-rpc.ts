// Live verification: hub RPC blocks until the agent's actual turn reply.
import { HubClient } from "../../src/irc/remote/client";

async function main() {
	const targetPid = Number(process.env.TARGET_PID ?? 0);
	if (!targetPid) { console.error("TARGET_PID required"); process.exit(1); }
	const client = await HubClient.connect({
		socketPath: "/home/dave/.omp/agent/hub.sock",
		identity: { agentId: "verifier", project: "rpc-verify", status: "running" as const, pid: process.pid },
	});
	if (!client) { console.error("FAIL: no hub client"); process.exit(1); }
	console.log("connected; firing RPC at Main/" + targetPid);
	const t0 = Date.now();
	const reply = await client.request(
		{ type: "text", text: "Reply with exactly: HUB_RPC_OK and nothing else." } as never,
		[{ agentId: "Main", pid: targetPid }] as never,
		110_000,
	);
	const dt = ((Date.now() - t0) / 1000).toFixed(1);
	console.log(`reply after ${dt}s:`, JSON.stringify(reply, null, 2));
	client.close();
}
main();
