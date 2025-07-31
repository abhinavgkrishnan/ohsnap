import {
  DB,
  getDbClient,
  getHubClient,
  MessageHandler,
  StoreMessageOperation,
  MessageReconciliation,
  RedisClient,
  HubEventProcessor,
  EventStreamHubSubscriber,
  EventStreamConnection,
  HubEventStreamConsumer,
  HubSubscriber,
  MessageState,
} from "@farcaster/shuttle";
import { CustomDb, migrateToLatest, Tables } from "./db";
import {
  bytesToHexString,
  getStorageUnitExpiry,
  getStorageUnitType,
  HubEvent,
  isCastAddMessage,
  isCastRemoveMessage,
  isReactionAddMessage,
  isReactionRemoveMessage,
  isLinkAddMessage,
  isLinkRemoveMessage,
  isIdRegisterOnChainEvent,
  isMergeOnChainHubEvent,
  isSignerOnChainEvent,
  isStorageRentOnChainEvent,
  Message,
  MessageType,
} from "@farcaster/hub-nodejs";
import { log } from "./log";
import { Command } from "@commander-js/extra-typings";
import { readFileSync } from "fs";
import {
  BACKFILL_FIDS,
  CONCURRENCY,
  HUB_HOST,
  HUB_SSL,
  MAX_FID,
  POSTGRES_URL,
  POSTGRES_SCHEMA,
  REDIS_URL,
  SHARD_INDEX,
  TOTAL_SHARDS,
  USE_STREAMING_RPCS_FOR_BACKFILL,
  SUBSCRIBE_RPC_TIMEOUT,
} from "./env";
import process from "node:process";
import url from "node:url";
import { ok, Result } from "neverthrow";
import { getQueue, getWorker } from "./worker";
import { Queue } from "bullmq";
import { bytesToHex, farcasterTimeToDate } from "./utils";

const hubId = "my-snapchain-shuttle";

export class SnapchainShuttleApp implements MessageHandler {
  private readonly db: CustomDb;
  private readonly dbSchema: string;
  private hubSubscriber: HubSubscriber;
  private streamConsumer: HubEventStreamConsumer;
  public redis: RedisClient;
  private readonly hubId;

  constructor(
    db: CustomDb,
    dbSchema: string,
    redis: RedisClient,
    hubSubscriber: HubSubscriber,
    streamConsumer: HubEventStreamConsumer,
  ) {
    this.db = db;
    this.dbSchema = dbSchema;
    this.redis = redis;
    this.hubSubscriber = hubSubscriber;
    this.hubId = hubId;
    this.streamConsumer = streamConsumer;
  }

  static create(
    dbUrl: string,
    dbSchema: string,
    redisUrl: string,
    hubUrl: string,
    totalShards: number,
    shardIndex: number,
    hubSSL = false,
  ) {
    const db = getDbClient(dbUrl, dbSchema) as CustomDb;
    const hub = getHubClient(hubUrl, { ssl: hubSSL });
    const redis = RedisClient.create(redisUrl);
    const eventStreamForWrite = new EventStreamConnection(redis.client);
    const eventStreamForRead = new EventStreamConnection(redis.client);
    const shardKey = totalShards === 0 ? "all" : `${shardIndex}`;
    
    // Fixed constructor call - matches the actual API
    const hubSubscriber = new EventStreamHubSubscriber(
      hubId,                    // label
      hub,                      // hubClient (the full HubClient object)
      shardIndex,               // shardIndex  
      eventStreamForWrite,      // eventStream
      redis,                    // redis
      shardKey,                 // shardKey
      log,                      // log
      undefined,                // eventTypes (undefined = all types)
      SUBSCRIBE_RPC_TIMEOUT,    // connectionTimeout
    );
    
    const streamConsumer = new HubEventStreamConsumer(hub, eventStreamForRead, shardKey);
  
    return new SnapchainShuttleApp(db, dbSchema, redis, hubSubscriber, streamConsumer);
  }

  // Custom hub event handler - this is where you can add custom logic
  async onHubEvent(event: HubEvent, txn: DB): Promise<boolean> {
    // Handle on-chain events (FID registrations, signer events, storage rent)
    if (isMergeOnChainHubEvent(event)) {
      const onChainEvent = event.mergeOnChainEventBody.onChainEvent;
      let body = {};
      
      if (isIdRegisterOnChainEvent(onChainEvent)) {
        body = {
          eventType: onChainEvent.idRegisterEventBody.eventType,
          from: bytesToHex(onChainEvent.idRegisterEventBody.from),
          to: bytesToHex(onChainEvent.idRegisterEventBody.to),
          recoveryAddress: bytesToHex(onChainEvent.idRegisterEventBody.recoveryAddress),
        };
        log.info(`📝 FID Registration: ${onChainEvent.fid} - ${JSON.stringify(body)}`);
      } else if (isSignerOnChainEvent(onChainEvent)) {
        body = {
          eventType: onChainEvent.signerEventBody.eventType,
          key: bytesToHex(onChainEvent.signerEventBody.key),
          keyType: onChainEvent.signerEventBody.keyType,
          metadata: bytesToHex(onChainEvent.signerEventBody.metadata),
          metadataType: onChainEvent.signerEventBody.metadataType,
        };
        log.info(`🔑 Signer Event: FID ${onChainEvent.fid} - ${JSON.stringify(body)}`);
      } else if (isStorageRentOnChainEvent(onChainEvent)) {
        body = {
          eventType: getStorageUnitType(onChainEvent),
          expiry: getStorageUnitExpiry(onChainEvent),
          units: onChainEvent.storageRentEventBody.units,
          payer: bytesToHex(onChainEvent.storageRentEventBody.payer),
        };
        log.info(`💾 Storage Rent: FID ${onChainEvent.fid} - ${onChainEvent.storageRentEventBody.units} units`);
      }
      
      try {
        await (txn as CustomDb)
          .insertInto("onchain_events")
          .values({
            fid: onChainEvent.fid,
            timestamp: new Date(onChainEvent.blockTimestamp * 1000),
            blockNumber: onChainEvent.blockNumber,
            logIndex: onChainEvent.logIndex,
            txHash: onChainEvent.transactionHash,
            type: onChainEvent.type,
            body: body,
          })
          .execute();
      } catch (e) {
        log.error("❌ Failed to insert onchain event", e);
      }
    }
    
    return false; // Don't skip any events
  }

  // Custom message handler - this is where your main business logic goes
  async handleMessageMerge(
    message: Message,
    txn: DB,
    operation: StoreMessageOperation,
    state: MessageState,
    isNew: boolean,
    wasMissed: boolean,
  ): Promise<void> {
    if (!isNew) {
      // Message was already processed, skip
      return;
    }

    if (!message.data) {
      log.warn("Message data is undefined, skipping");
      return;
    }

    const customDB = txn as unknown as CustomDb;
    const messageDesc = wasMissed ? `missed message (${operation})` : `message (${operation})`;
    
    // Handle Cast Messages
    const isCastMessage = isCastAddMessage(message) || isCastRemoveMessage(message);
    if (isCastMessage && state === "created") {
      const castText = message.data.castAddBody?.text || "";
      await customDB
        .insertInto("casts")
        .values({
          fid: message.data.fid,
          hash: message.hash,
          text: castText,
          timestamp: farcasterTimeToDate(message.data.timestamp) || new Date(),
          parentFid: message.data.castAddBody?.parentCastId?.fid || null,
          parentHash: message.data.castAddBody?.parentCastId?.hash || null,
          embedCount: message.data.castAddBody?.embeds?.length || 0,
          mentionCount: message.data.castAddBody?.mentions?.length || 0,
        })
        .execute();
      
      log.info(`📝 ${state} cast from FID ${message.data.fid}: "${castText.slice(0, 50)}${castText.length > 50 ? '...' : ''}"`);
      
    } else if (isCastMessage && state === "deleted") {
      await customDB
        .updateTable("casts")
        .set({ deletedAt: farcasterTimeToDate(message.data.timestamp) || new Date() })
        .where("hash", "=", message.hash)
        .execute();
      
      log.info(`🗑️ ${state} cast from FID ${message.data.fid}`);
    }

    // Handle Reaction Messages
    const isReactionMessage = isReactionAddMessage(message) || isReactionRemoveMessage(message);
    if (isReactionMessage && state === "created") {
      const reactionType = message.data.reactionBody?.type || 0;
      const targetFid = message.data.reactionBody?.targetCastId?.fid || null;
      const targetHash = message.data.reactionBody?.targetCastId?.hash || null;
      
      await customDB
        .insertInto("reactions")
        .values({
          fid: message.data.fid,
          hash: message.hash,
          type: reactionType,
          targetFid: targetFid,
          targetHash: targetHash,
          timestamp: farcasterTimeToDate(message.data.timestamp) || new Date(),
        })
        .execute();
      
      log.info(`❤️ ${state} reaction (type ${reactionType}) from FID ${message.data.fid} to FID ${targetFid}`);
      
    } else if (isReactionMessage && state === "deleted") {
      await customDB
        .updateTable("reactions")
        .set({ deletedAt: farcasterTimeToDate(message.data.timestamp) || new Date() })
        .where("hash", "=", message.hash)
        .execute();
      
      log.info(`💔 ${state} reaction from FID ${message.data.fid}`);
    }

    // Handle Link Messages (Follows)
    const isLinkMessage = isLinkAddMessage(message) || isLinkRemoveMessage(message);
    if (isLinkMessage && state === "created") {
      const linkType = message.data.linkBody?.type || "";
      const targetFid = message.data.linkBody?.targetFid || null;
      
      await customDB
        .insertInto("links")
        .values({
          fid: message.data.fid,
          hash: message.hash,
          type: linkType,
          targetFid: targetFid,
          timestamp: farcasterTimeToDate(message.data.timestamp) || new Date(),
        })
        .execute();
      
      log.info(`🔗 ${state} ${linkType} from FID ${message.data.fid} to FID ${targetFid}`);
      
    } else if (isLinkMessage && state === "deleted") {
      await customDB
        .updateTable("links")
        .set({ deletedAt: farcasterTimeToDate(message.data.timestamp) || new Date() })
        .where("hash", "=", message.hash)
        .execute();
      
      log.info(`⛓️‍💥 ${state} link from FID ${message.data.fid}`);
    }

    // Custom analytics/metrics you can add:
    if (isNew) {
      // Track message volume by type
      await customDB
        .insertInto("message_stats")
        .values({
          messageType: message.data.type,
          fid: message.data.fid,
          operation: operation,
          state: state,
          timestamp: farcasterTimeToDate(message.data.timestamp) || new Date(),
          blockNumber: Number(message.data.timestamp), // Using Farcaster timestamp as block reference
          wasMissed: wasMissed,
        })
        .execute();
    }

    const hashStr = bytesToHexString(message.hash)._unsafeUnwrap();
    log.debug(`✅ ${state} ${messageDesc} ${hashStr} (type ${message.data?.type})`);
  }

  async start() {
    log.info("🚀 Starting Snapchain Shuttle...");
    
    await this.ensureMigrations();
    
    // Start hub subscriber (writes events to Redis stream)
    await this.hubSubscriber.start();
    log.info("📡 Hub subscriber started");

    // Wait for stream to be established
    await new Promise((resolve) => setTimeout(resolve, 10_000));

    log.info("🔄 Starting stream consumer");
    // Start stream consumer (processes events from Redis)
    await this.streamConsumer.start(async (event) => {
      await this.processHubEvent(event);
      return ok({ skipped: false });
    });
  }

  async reconcileFids(fids: number[]) {
    log.info(`🔍 Starting reconciliation for ${fids.length} FIDs...`);
    const reconciler = new MessageReconciliation(
      this.hubSubscriber.hubClient!,
      this.db as DB,
      log,
      undefined, // connectionTimeout
    );
    
    for (const fid of fids) {
      const startTime = Date.now();
      await reconciler.reconcileMessagesForFid(
        fid,
        async (message, missingInDb, prunedInDb, revokedInDb) => {
          if (missingInDb) {
            await HubEventProcessor.handleMissingMessage(this.db as DB, message, this);
            log.debug(`📥 Backfilled missing message for FID ${fid}`);
          } else if (prunedInDb || revokedInDb) {
            const messageDesc = prunedInDb ? "pruned" : revokedInDb ? "revoked" : "existing";
            log.debug(`♻️ Reconciled ${messageDesc} message for FID ${fid}`);
          }
        },
        async (message, missingInHub) => {
          if (missingInHub) {
            log.warn(`⚠️ Message missing in hub for FID ${fid}`);
          }
        },
      );
      const elapsed = (Date.now() - startTime) / 1000;
      log.info(`✅ Reconciled FID ${fid} in ${elapsed.toFixed(2)}s`);
    }
  }

  async backfillFids(fids: number[], backfillQueue: Queue) {
    const startedAt = Date.now();
    if (fids.length === 0) {
      let maxFid = MAX_FID ? parseInt(MAX_FID) : undefined;
      if (!maxFid) {
        // Get info from hub to determine max FID
        const getInfoResult = await this.hubSubscriber.hubClient?.getInfo({});
        if (getInfoResult?.isErr()) {
          log.error("❌ Failed to get hub info", getInfoResult.error);
          throw getInfoResult.error;
        } else {
          // Try to get from dbStats, fallback to a reasonable default
          maxFid = getInfoResult?._unsafeUnwrap()?.dbStats?.numMessages || 100000;
          if (!maxFid) {
            log.error("❌ Failed to get max FID, using default");
            maxFid = 100000;
          }
        }
      }
      log.info(`📊 Queuing up FIDs up to: ${maxFid}`);
      
      // Create batches of FIDs for processing
      const batchSize = 10;
      const fidBatches = Array.from({ length: Math.ceil(maxFid / batchSize) }, (_, i) => i * batchSize)
        .map((fid) => fid + 1);
      
      for (const start of fidBatches) {
        const subset = Array.from({ length: batchSize }, (_, i) => start + i);
        await backfillQueue.add("reconcile", { fids: subset });
      }
    } else {
      await backfillQueue.add("reconcile", { fids });
    }
    
    await backfillQueue.add("completionMarker", { startedAt });
    log.info("📋 Backfill jobs queued");
  }

  private async processHubEvent(hubEvent: HubEvent) {
    await HubEventProcessor.processHubEvent(this.db as DB, hubEvent, this);
  }

  async ensureMigrations() {
    log.info("🔄 Running database migrations...");
    const result = await migrateToLatest(this.db as DB, this.dbSchema, log);
    if (result.isErr()) {
      log.error("❌ Failed to migrate database", result.error);
      throw result.error;
    }
    log.info("✅ Database migrations complete");
  }

  async stop() {
    log.info("🛑 Stopping Snapchain Shuttle...");
    this.hubSubscriber.stop();
    const lastEventId = await this.redis.getLastProcessedEvent(this.hubId);
    log.info(`📊 Stopped at eventId: ${lastEventId}`);
  }

  // Get some stats for monitoring
  async getStats() {
    const stats = await this.db
      .selectFrom("message_stats")
      .select([
        "messageType",
        this.db.fn.count("id").as("count"),
      ])
      .groupBy("messageType")
      .execute();
    
    const castCount = await this.db
      .selectFrom("casts")
      .select(this.db.fn.count("id").as("count"))
      .where("deletedAt", "is", null)
      .executeTakeFirst();
    
    const reactionCount = await this.db
      .selectFrom("reactions")
      .select(this.db.fn.count("id").as("count"))
      .where("deletedAt", "is", null)
      .executeTakeFirst();
    
    return {
      messagesByType: stats,
      activeCasts: castCount?.count || 0,
      activeReactions: reactionCount?.count || 0,
    };
  }
}

// Command line interface
if (import.meta.url.endsWith(url.pathToFileURL(process.argv[1] || "").toString())) {
  async function start() {
    log.info(`🌟 Creating Snapchain Shuttle app`);
    log.info(`📡 Connecting to: ${POSTGRES_URL}, ${REDIS_URL}, ${HUB_HOST}`);
    log.info(`🔢 Using ${TOTAL_SHARDS} shards, processing shard ${SHARD_INDEX}`);
    
    const app = SnapchainShuttleApp.create(
      POSTGRES_URL, 
      POSTGRES_SCHEMA, 
      REDIS_URL, 
      HUB_HOST, 
      TOTAL_SHARDS, 
      SHARD_INDEX, 
      HUB_SSL
    );
    
    // Add graceful shutdown
    process.on('SIGINT', async () => {
      log.info("🛑 Received SIGINT, shutting down gracefully...");
      await app.stop();
      process.exit(0);
    });
    
    await app.start();
  }

  async function backfill() {
    log.info(`🔄 Starting backfill process`);
    const app = SnapchainShuttleApp.create(
      POSTGRES_URL, 
      POSTGRES_SCHEMA, 
      REDIS_URL, 
      HUB_HOST, 
      TOTAL_SHARDS, 
      SHARD_INDEX, 
      HUB_SSL
    );
    
    const fids = BACKFILL_FIDS ? BACKFILL_FIDS.split(",").map((fid) => parseInt(fid)) : [];
    log.info(`📋 Backfilling FIDs: ${fids.length > 0 ? fids.join(", ") : "all"}`);
    
    const backfillQueue = getQueue(app.redis.client);
    await app.backfillFids(fids, backfillQueue);

    // Start the worker after initiating backfill
    const worker = getWorker(app, app.redis.client, log, CONCURRENCY);
    await worker.run();
  }

  async function worker() {
    log.info(`👷 Starting backfill worker with concurrency ${CONCURRENCY}`);
    const app = SnapchainShuttleApp.create(
      POSTGRES_URL, 
      POSTGRES_SCHEMA, 
      REDIS_URL, 
      HUB_HOST, 
      TOTAL_SHARDS, 
      SHARD_INDEX, 
      HUB_SSL
    );
    
    const worker = getWorker(app, app.redis.client, log, CONCURRENCY);
    await worker.run();
  }

  async function stats() {
    log.info(`📊 Getting stats...`);
    const app = SnapchainShuttleApp.create(
      POSTGRES_URL, 
      POSTGRES_SCHEMA, 
      REDIS_URL, 
      HUB_HOST, 
      TOTAL_SHARDS, 
      SHARD_INDEX, 
      HUB_SSL
    );
    
    await app.ensureMigrations();
    const stats = await app.getStats();
    console.log(JSON.stringify(stats, null, 2));
    process.exit(0);
  }

  const program = new Command()
    .name("snapchain-shuttle")
    .description("Custom Farcaster Snapchain data sync to PostgreSQL")
    .version("1.0.0");

  program.command("start").description("Start the shuttle sync process").action(start);
  program.command("backfill").description("Queue up backfill jobs for worker").action(backfill);
  program.command("worker").description("Start the backfill worker").action(worker);
  program.command("stats").description("Show database stats").action(stats);

  program.parse(process.argv);
}