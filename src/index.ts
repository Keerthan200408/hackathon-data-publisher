import * as dotenv from "dotenv";
dotenv.config();

import { config } from "./config";
import * as db from "./db";
import * as mqttClient from "./mqtt/client";
import * as messageProcessor from "./mqtt/messageProcessor";
import * as subscriptionManager from "./mqtt/subscriptionManager";

let isShuttingDown = false; // Flag to prevent multiple shutdowns
let client: ReturnType<typeof mqttClient.createClient> | null = null; // Store MQTT client reference

// Handle unhandled promise rejections
process.on("unhandledRejection", (reason, promise) => {
  console.error("Unhandled Rejection at:", promise, "reason:", reason);
  // Optionally exit the process if the error is critical
  process.exit(1);
});

async function start() {
  try {
    // Initialize the database
    const pool = db.createPool();
    db.initialize(pool);
    await db.initialize(pool); // Await database initialization
    // Initialize subscription manager
    subscriptionManager.initializeFirstMessageTracking();

    // Create MQTT client
    const client = mqttClient.createClient();

    // MQTT event handlers
    client.on("connect", () => {
      console.log("Connected to MQTT broker");

      // Subscribe to all index topics
      // subscriptionManager.subscribeToAllIndices(client);
      subscriptionManager.subscribeToAllIndices(client!);
    });

    client.on("message", (topic: string, message: Buffer) => {
      messageProcessor.processMessage(topic, message, client);
    });

    client.on("error", (err: Error) => {
      console.error("MQTT connection error:", err);
    });

    client.on("reconnect", () => {
      console.log("Attempting to reconnect to MQTT broker");
    });

    client.on("close", () => {
      console.log("MQTT connection closed");
    });

    // Handle graceful shutdown
    // process.on("SIGINT", async () => {
    //   await db.cleanupDatabase();
    //   client.end();
    //   process.exit();

    // Handle graceful shutdown
    process.on("SIGINT", async () => {
      if (isShuttingDown) {
        console.log("Shutdown already in progress, ignoring SIGINT");
        return;
      }
      isShuttingDown = true;

      console.log("Received SIGINT. Performing cleanup...");
      try {
        if (client) {
          client.end();
        }
        await db.cleanupDatabase();
        console.log("Shutdown completed successfully");
        process.exit(0);
      } catch (error) {
        console.error("Error during shutdown:", error);
        process.exit(1);
      }
    });
  } catch (error) {
    console.error("Application error:", error);
    process.exit(1);
  }
}

start();
