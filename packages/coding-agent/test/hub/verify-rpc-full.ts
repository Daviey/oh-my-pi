import { HubClient } from "@oh-my-pi/pi-coding-agent/irc/remote/client";
import * as path from "node:path";
import * as os from "node:os";

async function main() {
    const socketPath = path.join(os.homedir(), ".omp/agent/hub.sock");
    const client = await HubClient.connect({
        socketPath,
        identity: { agentId: "verifier", project: "test", status: "running", pid: process.pid }
    });

    if (!client) {
        console.error("Failed to connect to hub");
        process.exit(1);
    }

    // 1. Prepare to catch the asynchronous turn result
    const turnResultPromise = new Promise((resolve) => {
        client.onDelivery((msg) => {
            console.log("Async turn result received:", JSON.stringify(msg, null, 2));
            resolve(msg);
        });
    });

    // 2. Trigger steering via RPC
    console.log("Sending steering RPC request...");
    const reply = await client.request(
        { type: "text", text: "ping" } as any, 
        [{ agentId: "Main", pid: 1476456 }]
    );
    
    console.log("Immediate RPC receipt:", JSON.stringify(reply, null, 2));
    
    // 3. Wait for the actual turn result (async)
    await turnResultPromise;
    client.close();
}
main();
