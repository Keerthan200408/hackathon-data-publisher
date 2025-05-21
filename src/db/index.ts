import { Pool } from "pg";
import { config } from "../config";

// Define a type for batch items
export interface BatchItem {
  topic: string;
  ltp: number;
  indexName?: string;
  type?: string;
  strike?: number;
}

// Initialize database connection pool
let pool: Pool;
let dataBatch: BatchItem[] = [];
let batchTimer: NodeJS.Timeout | null = null;
let isFlushing = false; // Flag to prevent overlapping flushes
let isPoolEnded = false; // Flag to prevent multiple pool.end() calls
// Cache topic IDs to avoid repeated lookups
const topicCache = new Map<string, number>();

export function createPool(): Pool {
  pool = new Pool({
    host: config.db.host,
    port: config.db.port,
    user: config.db.user,
    password: config.db.password,
    database: config.db.database,
    max: 20, // Limit the pool to 20 connections
    idleTimeoutMillis: 10000, // Close idle connections after 30 seconds
    connectionTimeoutMillis: 5000, // Timeout connection attempts after 2 seconds
  });

  pool.on("error", (err) => {
    console.error("Unexpected error on idle client:", err);
  });
  pool.on("connect", () => {
    console.log("New database connection established");
  });
  pool.on("remove", () => {
    console.log("Database connection removed (idle timeout or error)");
  });

  return pool;
}

export async function initialize(dbPool: Pool): Promise<void> {
  pool = dbPool;
  

  // TODO: Preload topic cache from database
  try {
    await pool.query(`
      CREATE TABLE IF NOT EXISTS topics (
        topic_id SERIAL PRIMARY KEY,
        topic_name TEXT UNIQUE NOT NULL,
        index_name TEXT,
        type TEXT,
        strike INTEGER
      );

      CREATE TABLE IF NOT EXISTS ltp_data (
        id SERIAL PRIMARY KEY,
        topic_id INTEGER REFERENCES topics(topic_id),
        ltp DOUBLE PRECISION NOT NULL,
        received_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      );
    `);
    console.log("Database initialized");

    const result = await pool.query("SELECT topic_id, topic_name FROM topics");
    for (const row of result.rows) {
      topicCache.set(row.topic_name, row.topic_id);
    }
    console.log(`Preloaded ${topicCache.size} topics into cache`);
  } catch (error) {
    console.error("Error preloading topic cache:", error);
    throw error;
  }
}


async function withRetry<T>(operation: () => Promise<T>, retries: number = 3, delay: number = 1000): Promise<T> {
  for (let attempt = 1; attempt <= retries; attempt++) {
    try {
      return await operation();
    } catch (error) {
      if (attempt === retries){
        throw error;
      // console.warn(`Retry ${attempt}/${retries} failed:`, error.message);
      }
      await new Promise(resolve => setTimeout(resolve, delay));
    }
  }
  throw new Error("Max retries reached");
}


export async function getTopicId(
  topicName: string,
  indexName?: string,
  type?: string,
  strike?: number
): Promise<number> {
  // TODO: Implement this function
  // 1. Check if topic exists in cache
  // 2. If not in cache, check if it exists in database
  // 3. If not in database, insert it
  // 4. Return topic_id
  // 1. Check if topic exists in cache
  if (topicCache.has(topicName)) {
    return topicCache.get(topicName)!;
  }

  // // 2. If not in cache, check if it exists in database
  // const client = await pool.connect();
  // try {
  //   const result = await client.query(
  //     "SELECT topic_id FROM topics WHERE topic_name = $1",
  //     [topicName]
  //   );

  //   if (result.rows.length > 0) {
  //     const topicId = result.rows[0].topic_id;
  //     topicCache.set(topicName, topicId);
  //     return topicId;
  //   }

  //   // 3. If not in database, insert it
  //   const insertResult = await client.query(
  //     "INSERT INTO topics (topic_name, index_name, type, strike) VALUES ($1, $2, $3, $4) RETURNING topic_id",
  //     [topicName, indexName || null, type || null, strike || null]
  //   );

  //   const topicId = insertResult.rows[0].topic_id;
  //   topicCache.set(topicName, topicId);
  //   return topicId;
  // } catch (error) {
  //   console.error(`Error getting topic ID for ${topicName}:`, error);
  //   throw error;

  return await withRetry(async () => {
    const result = await pool.query(
      "SELECT topic_id FROM topics WHERE topic_name = $1",
      [topicName]
    );

    if (result.rows.length > 0) {
      const topicId = result.rows[0].topic_id;
      topicCache.set(topicName, topicId);
      return topicId;
    }

    const insertResult = await pool.query(
      "INSERT INTO topics (topic_name, index_name, type, strike) VALUES ($1, $2, $3, $4) RETURNING topic_id",
      [topicName, indexName || null, type || null, strike || null]
    );

    const topicId = insertResult.rows[0].topic_id;
    topicCache.set(topicName, topicId);
    return topicId;
  });


  // } finally {
  //   client.release();
  // }
}

export function saveToDatabase(
  topic: string,
  ltp: number,
  indexName?: string,
  type?: string,
  strike?: number
) {
  // TODO: Implement this function
  // 1. Add item to batch
  // 2. If batch timer is not running, start it
  // 3. If batch size reaches threshold, flush batch
  // 1. Add item to batch
  const batchItem: BatchItem = { topic, ltp, indexName, type, strike };
  dataBatch.push(batchItem);
  console.log(
    `Added to batch: ${topic}, LTP: ${ltp}, Batch size: ${dataBatch.length}`
  );

  // 2. If batch timer is not running, start it
  if (!batchTimer) {
    batchTimer = setTimeout(async () => {
      await flushBatch();
    }, config.app.batchInterval);
  }

  // 3. If batch size reaches threshold, flush batch
  if (dataBatch.length >= config.app.batchSize) {
    clearTimeout(batchTimer!);
    batchTimer = null;
    flushBatch();
  }
  console.log(`Saving to database: ${topic}, LTP: ${ltp}`);
}

export async function flushBatch() {
  // TODO: Implement this function
  // 1. Clear timer
  // 2. If batch is empty, return
  // 3. Process batch items (get topic IDs)
  // 4. Insert data in a transaction
  // 5. Reset batch

  // 1. Clear timer
  if (isFlushing) {
    console.log("Flush already in progress, skipping...");
    return;
  }

  if (batchTimer) {
    clearTimeout(batchTimer);
    batchTimer = null;
  }

  // 2. If batch is empty, return
  if (dataBatch.length === 0) {
    console.log("Batch is empty, nothing to flush");
    return;
  }
  console.log("Flushing batch to database");
  
  // // 3. Process batch items (get topic IDs)
  // const client = await pool.connect();
  // try {
  //   await client.query("BEGIN");

  //   for (const item of dataBatch) {
  //     const topicId = await getTopicId(
  //       item.topic,
  //       item.indexName,
  //       item.type,
  //       item.strike
  //     );

  //     // 4. Insert data in a transaction
  //     await client.query(
  //       "INSERT INTO ltp_data (topic_id, ltp, received_at) VALUES ($1, $2, NOW())",
  //       [topicId, item.ltp]
  //     );
  //   }

  //   await client.query("COMMIT");
  //   console.log(`Successfully flushed ${dataBatch.length} items`);
  // } catch (error) {
  //   await client.query("ROLLBACK");
  //   console.error("Error flushing batch:", error);
  //   throw error;
  // } finally {
  //   client.release();
  //   // 5. Reset batch
  //   dataBatch = [];
  // }

  const batchToFlush = [...dataBatch]; // Create a copy of the batch to flush
  try {
    // Process batch items (get topic IDs and insert data)
    for (const item of batchToFlush) {
      const topicId = await getTopicId(
        item.topic,
        item.indexName,
        item.type,
        item.strike
      );

      await pool.query(
        "INSERT INTO ltp_data (topic_id, ltp, received_at) VALUES ($1, $2, NOW())",
        [topicId, item.ltp]
      );
    }

    console.log(`Successfully flushed ${batchToFlush.length} items`);
  } catch (error) {
    console.error("Error flushing batch to database:", error);
    // Log the specific item that caused the error, if possible
    console.error("Failed batch items:", batchToFlush);
    // Do not rethrow; allow the application to continue
  } finally {
    // Reset batch and flush state
    dataBatch = dataBatch.slice(batchToFlush.length); // Remove only the items that were processed
    isFlushing = false;
  }
}

export async function cleanupDatabase() {
  // Flush any remaining items in the batch
  if (dataBatch.length > 0) {
    await flushBatch();
  }

  // Close the database pool
  if (pool) {
    await pool.end();
  }

  console.log("Database cleanup completed");
}
