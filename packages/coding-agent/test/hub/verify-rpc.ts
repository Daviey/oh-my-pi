import { HubClient } from "@oh-my-pi/pi-coding-agent/irc/remote/client";
import * as path from "node:path";
import * as os from "node:os";

async function main() {
    const socketPath = path.join(os.homedir(), ".omp/agent/hub.sock");
    const client = await HubClient.connect({
        socketPath,
        identity: { agentId: "verify", project: "test", status: "running", pid: process.pid }
    });

    if (!client) {
        console.error("Failed to connect to hub");
        process.exit(1);
    }

    console.log("Connected to hub. Requesting ping from rust-kernel Main...");
    
    // Targeting PID 1476456 (rust-kernel Main)
    const reply = await client.request(
        { type: "text", text: "ping" } as any, 
        [{ agentId: "Main", pid: 1476456 }]
    );
    
    if (reply) {
        console.log("Reply received:", JSON.stringify(reply, null, 2));
    } else {
        console.log("Timed out or no peer matched.");
    }
    client.close();
}
main();
