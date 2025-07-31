// src/worker.ts
import { Cluster, Redis } from "ioredis";
import { Job, Queue, Worker } from "bullmq";
import { SnapchainShuttleApp } from "./app";
import { pino } from "pino";

const QUEUE_NAME = "snapchain-reconcile";

export function getWorker(app: SnapchainShuttleApp, redis: Redis | Cluster, log: pino.Logger, concurrency = 1) {
  const worker = new Worker(
    QUEUE_NAME,
    async (job: Job) => {
      if (job.name === "reconcile") {
        const start = Date.now();
        const fids = job.data.fids as number[];
        
        log.info(`🔄 Starting reconciliation for ${fids.length} FIDs: ${fids.join(", ")}`);
        await app.reconcileFids(fids);
        
        const elapsed = (Date.now() - start) / 1000;
        const lastFid = fids[fids.length - 1];
        const avgTimePerFid = elapsed / fids.length;
        
        log.info(`✅ Reconciled ${fids.length} FIDs (up to ${lastFid}) in ${elapsed.toFixed(2)}s (${avgTimePerFid.toFixed(2)}s/FID)`);
        
      } else if (job.name === "completionMarker") {
        const startedAt = new Date(job.data.startedAt as number);
        const duration = (Date.now() - startedAt.getTime()) / 1000 / 60;
        
        log.info(`🎉 Reconciliation completed!`);
        log.info(`⏱️  Started: ${startedAt.toISOString()}`);
        log.info(`⏱️  Finished: ${new Date().toISOString()}`);
        log.info(`⏱️  Total time: ${duration.toFixed(2)} minutes`);
        
        // You could add notifications here
        // await sendSlackNotification(`Snapchain reconciliation completed in ${duration.toFixed(2)} minutes`);
      }
    },
    {
      autorun: false,
      useWorkerThreads: concurrency > 1,
      concurrency,
      connection: redis,
      removeOnComplete: { count: 50 },
      removeOnFail: { count: 50 },
      settings: {
        backoffStrategy: (attemptsMade) => {
          return Math.min(Math.pow(2, attemptsMade) * 1000, 30000);
        },
      },
    },
  );

  worker.on("completed", (job) => {
    log.debug(`✅ Job ${job.name} completed`);
  });

  worker.on("failed", (job, err) => {
    log.error(`❌ Job ${job?.name} failed:`, err);
  });

  worker.on("stalled", (jobId) => {
    log.warn(`⚠️  Job ${jobId} stalled`);
  });

  return worker;
}

export function getQueue(redis: Redis | Cluster) {
  return new Queue(QUEUE_NAME, {
    connection: redis,
    defaultJobOptions: { 
      attempts: 3, 
      backoff: { 
        delay: 2000, 
        type: "exponential" 
      },
      removeOnComplete: 50,
      removeOnFail: 50,
    },
  });
}