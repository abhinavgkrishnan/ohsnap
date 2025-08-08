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
import { childLogger, log, Logger, sinceProcessStartMs, sinceProcessStartMin } from "./log";
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

  async onHubEvent(event: HubEvent, txn: DB): Promise<boolean> {
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
        log.info(`FID Registration: ${onChainEvent.fid} - ${JSON.stringify(body)}`);
      } else if (isSignerOnChainEvent(onChainEvent)) {
        body = {
          eventType: onChainEvent.signerEventBody.eventType,
          key: bytesToHex(onChainEvent.signerEventBody.key),
          keyType: onChainEvent.signerEventBody.keyType,
          metadata: bytesToHex(onChainEvent.signerEventBody.metadata),
          metadataType: onChainEvent.signerEventBody.metadataType,
        };
        log.info(`Signer Event: FID ${onChainEvent.fid} - ${JSON.stringify(body)}`);
      } else if (isStorageRentOnChainEvent(onChainEvent)) {
        body = {
          eventType: getStorageUnitType(onChainEvent),
          expiry: getStorageUnitExpiry(onChainEvent),
          units: onChainEvent.storageRentEventBody.units,
          payer: bytesToHex(onChainEvent.storageRentEventBody.payer),
        };
        log.info(`Storage Rent: FID ${onChainEvent.fid} - ${onChainEvent.storageRentEventBody.units} units`);
      }
      
      try {
        const t0 = Date.now();
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
        const durMs = Date.now() - t0;
        log.trace({ event: "db_exec", table: "onchain_events", sql_kind: "insert", dur_ms: durMs, t_since_proc_start_ms: sinceProcessStartMs(), t_since_proc_start_min: sinceProcessStartMin() }, "Inserted onchain event");
      } catch (e) {
        log.error("Failed to insert onchain event", e);
      }
    }
    
    return false;
  }

  async handleMessageMerge(
    message: Message,
    txn: DB,
    operation: StoreMessageOperation,
    state: MessageState,
    isNew: boolean,
    wasMissed: boolean,
  ): Promise<void> {
    if (!isNew) {
      return;
    }

    if (!message.data) {
      log.warn("Message data is undefined, skipping");
      return;
    }

    const customDB = txn as unknown as CustomDb;
    const messageDesc = wasMissed ? `missed message (${operation})` : `message (${operation})`;
    
    // // Handle Cast Messages
    // const isCastMessage = isCastAddMessage(message) || isCastRemoveMessage(message);
    // if (isCastMessage && state === "created") {
    //   const castText = message.data.castAddBody?.text || "";
    //   await customDB
    //     .insertInto("casts")
    //     .values({
    //       fid: message.data.fid,
    //       hash: message.hash,
    //       text: castText,
    //       timestamp: farcasterTimeToDate(message.data.timestamp) || new Date(),
    //       parentFid: message.data.castAddBody?.parentCastId?.fid || null,
    //       parentHash: message.data.castAddBody?.parentCastId?.hash || null,
    //       embedCount: message.data.castAddBody?.embeds?.length || 0,
    //       mentionCount: message.data.castAddBody?.mentions?.length || 0,
    //     })
    //     .execute();
      
    //   log.info(`Cast ${state} from FID ${message.data.fid}: "${castText.slice(0, 50)}${castText.length > 50 ? '...' : ''}"`);
      
    // } else if (isCastMessage && state === "deleted") {
    //   await customDB
    //     .updateTable("casts")
    //     .set({ deletedAt: farcasterTimeToDate(message.data.timestamp) || new Date() })
    //     .where("hash", "=", message.hash)
    //     .execute();
      
    //   log.info(`Cast ${state} from FID ${message.data.fid}`);
    // }

    // Handle Reaction Messages - COMMENTED OUT FOR PHASE 1 BACKFILL
    // const isReactionMessage = isReactionAddMessage(message) || isReactionRemoveMessage(message);
    // if (isReactionMessage && state === "created") {
    //   const reactionType = message.data.reactionBody?.type || 0;
    //   const targetFid = message.data.reactionBody?.targetCastId?.fid || null;
    //   const targetHash = message.data.reactionBody?.targetCastId?.hash || null;
      
    //   await customDB
    //     .insertInto("reactions")
    //     .values({
    //       fid: message.data.fid,
    //       hash: message.hash,
    //       type: reactionType,
    //       targetFid: targetFid,
    //       targetHash: targetHash,
    //       timestamp: farcasterTimeToDate(message.data.timestamp) || new Date(),
    //     })
    //     .execute();
      
    //   log.info(`Reaction ${state} (type ${reactionType}) from FID ${message.data.fid} to FID ${targetFid}`);
      
    // } else if (isReactionMessage && state === "deleted") {
    //   await customDB
    //     .updateTable("reactions")
    //     .set({ deletedAt: farcasterTimeToDate(message.data.timestamp) || new Date() })
    //     .where("hash", "=", message.hash)
    //     .execute();
      
    //   log.info(`Reaction ${state} from FID ${message.data.fid}`);
    // }

    // Handle Link Messages (Follows) - COMMENTED OUT FOR PHASE 1 BACKFILL
    // const isLinkMessage = isLinkAddMessage(message) || isLinkRemoveMessage(message);
    // if (isLinkMessage && state === "created") {
    //   const linkType = message.data.linkBody?.type || "";
    //   const targetFid = message.data.linkBody?.targetFid || null;
      
    //   await customDB
    //     .insertInto("links")
    //     .values({
    //       fid: message.data.fid,
    //       hash: message.hash,
    //       type: linkType,
    //       targetFid: targetFid,
    //       timestamp: farcasterTimeToDate(message.data.timestamp) || new Date(),
    //     })
    //     .execute();
      
    //   log.info(`Link ${state} ${linkType} from FID ${message.data.fid} to FID ${targetFid}`);
      
    // } else if (isLinkMessage && state === "deleted") {
    //   await customDB
    //     .updateTable("links")
    //     .set({ deletedAt: farcasterTimeToDate(message.data.timestamp) || new Date() })
    //     .where("hash", "=", message.hash)
    //     .execute();
      
    //   log.info(`Link ${state} from FID ${message.data.fid}`);
    // }

    // Keep message stats for analytics
    if (isNew) {
      const t0 = Date.now();
      await customDB
        .insertInto("message_stats")
        .values({
          messageType: message.data.type,
          fid: message.data.fid,
          operation: operation,
          state: state,
          timestamp: farcasterTimeToDate(message.data.timestamp) || new Date(),
          blockNumber: Number(message.data.timestamp),
          wasMissed: wasMissed,
        })
        .execute();
      const durMs = Date.now() - t0;
      log.trace({ event: "db_exec", table: "message_stats", sql_kind: "insert", dur_ms: durMs, t_since_proc_start_ms: sinceProcessStartMs(), t_since_proc_start_min: sinceProcessStartMin() }, "Inserted message_stats row");
    }

    const hashStr = bytesToHexString(message.hash)._unsafeUnwrap();
    log.debug(`Processed ${state} ${messageDesc} ${hashStr} (type ${message.data?.type})`);
  }

  async start() {
    log.info("Starting Snapchain Shuttle...");
    
    await this.ensureMigrations();
    
    await this.hubSubscriber.start();
    log.info("Hub subscriber started");

    await new Promise((resolve) => setTimeout(resolve, 10_000));

    log.info("Starting stream consumer");
    await this.streamConsumer.start(async (event) => {
      await this.processHubEvent(event);
      return ok({ skipped: false });
    });
  }

  async reconcileFids(fids: number[], ctx?: { jobLog?: Logger; batchStart?: number }) {
    const batchLog = (ctx?.jobLog || log).child({ component: "reconcile" });
    const batchStart = ctx?.batchStart ?? Date.now();
    batchLog.info({ event: "reconcile_start", fids_count: fids.length, t_since_proc_start_ms: sinceProcessStartMs(), t_since_proc_start_min: sinceProcessStartMin() }, `Starting reconciliation for ${fids.length} FIDs...`);
    const reconciler = new MessageReconciliation(
      this.hubSubscriber.hubClient!,
      this.db as DB,
      batchLog,
      undefined,
    );
    
    for (const fid of fids) {
      const fidStart = Date.now();
      const fidLog = batchLog.child({ fid });
      let dbMsTotal = 0;
      let messagesFound = 0;
      let messagesProcessed = 0;
      let bytesInTotal = 0;
      
      const grpcStart = Date.now();
      await reconciler.reconcileMessagesForFid(
        fid,
        async (message, missingInDb, prunedInDb, revokedInDb) => {
          messagesFound++;
          try { bytesInTotal += JSON.stringify(message).length; } catch {}
          if (missingInDb) {
            const dbT0 = Date.now();
            await HubEventProcessor.handleMissingMessage(this.db as DB, message, this);
            dbMsTotal += Date.now() - dbT0;
            messagesProcessed++;
            fidLog.trace({ event: "message_backfilled" }, `Backfilled missing message`);
          } else if (prunedInDb || revokedInDb) {
            const messageDesc = prunedInDb ? "pruned" : revokedInDb ? "revoked" : "existing";
            fidLog.trace({ event: "message_reconciled", message_desc: messageDesc }, `Reconciled ${messageDesc} message`);
          }
        },
        async (message, missingInHub) => {
          if (missingInHub) {
            fidLog.warn({ event: "message_missing_in_hub" }, `Message missing in hub`);
          }
        },
      );
      const grpcMsTotal = Date.now() - grpcStart - dbMsTotal; // Total time minus DB operations
      
      const elapsedMs = Date.now() - fidStart;
      fidLog.info({
        event: "fid_done",
        dur_ms: elapsedMs,
        grpc_fetch_ms: grpcMsTotal,
        db_ms: dbMsTotal,
        messages_found: messagesFound,
        messages_processed: messagesProcessed,
        bytes_in: bytesInTotal,
        t_since_proc_start_ms: sinceProcessStartMs(),
        t_since_proc_start_min: sinceProcessStartMin(),
      }, `Reconciled FID ${fid} in ${(elapsedMs / 1000).toFixed(2)}s`);
    }
  }

  async backfillFids(fids: number[], backfillQueue: Queue) {
    const startedAt = Date.now();
    const backfillLog = log.child({ component: "backfill" });
    if (fids.length === 0) {
      let maxFid = MAX_FID ? parseInt(MAX_FID) : undefined;
      if (!maxFid) {
        // Get info from hub to determine max FID
        const rpcStart = Date.now();
        const getInfoResult = await this.hubSubscriber.hubClient?.getInfo({});
        const rpcDurMs = Date.now() - rpcStart;
        if (getInfoResult?.isErr()) {
          backfillLog.error({ event: "grpc_call", method: "getInfo", status: "error", dur_ms: rpcDurMs }, "Failed to get hub info");
          throw getInfoResult.error;
        } else {
          // Try to get from dbStats, fallback to a reasonable default
          const info = getInfoResult?._unsafeUnwrap();
          maxFid = (info?.dbStats as any)?.numFidRegistrations || 100000;
          backfillLog.info({ event: "grpc_call", method: "getInfo", status: "ok", dur_ms: rpcDurMs, response_bytes: (()=>{ try { return JSON.stringify(info).length } catch { return undefined } })() }, "Got hub info");
          if (!maxFid) {
            backfillLog.error({ event: "max_fid_missing" }, "Failed to get max FID, using default");
            maxFid = 100000;
          }
        }
      }
      backfillLog.info({ event: "queue_begin", max_fid: maxFid }, `Queuing up FIDs up to: ${maxFid}`);
      
      // Create batches of FIDs for processing
      const batchSize = 1000;
      const fidBatches = Array.from({ length: Math.ceil(maxFid / batchSize) }, (_, i) => i * batchSize)
        .map((fid) => fid + 1);
      
      let enqueued = 0;
      const qStart = Date.now();
      for (const start of fidBatches) {
        const subset = Array.from({ length: batchSize }, (_, i) => start + i);
        const payload = { fids: subset };
        const payloadBytes = JSON.stringify(payload).length;
        const addStart = Date.now();
        const job = await backfillQueue.add("reconcile", payload);
        const addDurMs = Date.now() - addStart;
        enqueued += subset.length;
        backfillLog.trace({ event: "queue_add_batch", batch_size: subset.length, fid_min: subset[0], fid_max: subset[subset.length - 1], payload_bytes: payloadBytes, dur_ms: addDurMs, jobId: job.id }, "Enqueued batch");
      }
      const qDurMs = Date.now() - qStart;
      backfillLog.info({ event: "queue_done", batches: fidBatches.length, fids_enqueued: enqueued, dur_ms: qDurMs, fids_per_s: Number((enqueued / (qDurMs/1000)).toFixed(2)) }, "Finished enqueuing backfill batches");
    } else {
      const payload = { fids };
      const payloadBytes = JSON.stringify(payload).length;
      const addStart = Date.now();
      const job = await backfillQueue.add("reconcile", payload);
      const addDurMs = Date.now() - addStart;
      backfillLog.info({ event: "queue_add", batch_size: fids.length, fid_min: Math.min(...fids), fid_max: Math.max(...fids), payload_bytes: payloadBytes, dur_ms: addDurMs, jobId: job.id }, "Enqueued single backfill batch");
    }
    
    await backfillQueue.add("completionMarker", { startedAt });
    backfillLog.info({ event: "queue_completion_marker" }, "Backfill jobs queued");
  }

  private async processHubEvent(hubEvent: HubEvent) {
    await HubEventProcessor.processHubEvent(this.db as DB, hubEvent, this);
  }

  async ensureMigrations() {
    log.info("Running database migrations...");
    const result = await migrateToLatest(this.db as DB, this.dbSchema, log);
    if (result.isErr()) {
      log.error("Failed to migrate database", result.error);
      throw result.error;
    }
    log.info("Database migrations complete");
  }

  async stop() {
    log.info("Stopping Snapchain Shuttle...");
    this.hubSubscriber.stop();
    const lastEventId = await this.redis.getLastProcessedEvent(this.hubId);
    log.info(`Stopped at eventId: ${lastEventId}`);
  }

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
    log.info(`Creating Snapchain Shuttle app`);
    log.info(`Connecting to: ${POSTGRES_URL}, ${REDIS_URL}, ${HUB_HOST}`);
    log.info(`Using ${TOTAL_SHARDS} shards, processing shard ${SHARD_INDEX}`);
    
    const app = SnapchainShuttleApp.create(
      POSTGRES_URL, 
      POSTGRES_SCHEMA, 
      REDIS_URL, 
      HUB_HOST, 
      TOTAL_SHARDS, 
      SHARD_INDEX, 
      HUB_SSL
    );
    
    process.on('SIGINT', async () => {
      log.info("Received SIGINT, shutting down gracefully...");
      await app.stop();
      process.exit(0);
    });
    
    await app.start();
  }

  async function backfill() {
    log.info(`Starting backfill process`);
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
    log.info(`Backfilling FIDs: ${fids.length > 0 ? fids.join(", ") : "all"}`);
    
    const backfillQueue = getQueue(app.redis.client);
    await app.backfillFids(fids, backfillQueue);
    
    log.info("Backfill jobs queued successfully. Run 'npm run worker' to process them.");
  }

  async function worker() {
    log.info(`Starting backfill worker with concurrency ${CONCURRENCY}`);
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
    log.info(`Getting stats...`);
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