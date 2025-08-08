// src/worker.ts
import { Cluster, Redis } from "ioredis";
import { Job, Queue, Worker } from "bullmq";
import { SnapchainShuttleApp } from "./app";
import { pino } from "pino";
import { childLogger, sinceProcessStartMin, sinceProcessStartMs } from "./log";

const QUEUE_NAME = "snapchain-reconcile";

export function getWorker(app: SnapchainShuttleApp, redis: Redis | Cluster, log: pino.Logger, concurrency = 1) {
  const baseWorkerLog = childLogger({ component: "worker", concurrency });
  const worker = new Worker(
    QUEUE_NAME,
    async (job: Job) => {
      const jobLog = baseWorkerLog.child({ jobId: job.id, name: job.name });
      if (job.name === "reconcile") {
        const batchStart = Date.now();
        const fids = job.data.fids as number[];
        const fidMin = Math.min(...fids);
        const fidMax = Math.max(...fids);

        jobLog.info({
          event: "batch_start",
          fids_count: fids.length,
          fid_min: fidMin,
          fid_max: fidMax,
          payload_bytes: JSON.stringify(job.data)?.length || 0,
          t_since_proc_start_ms: sinceProcessStartMs(),
          t_since_proc_start_min: sinceProcessStartMin(),
        }, `Starting reconciliation for ${fids.length} FIDs`);

        await app.reconcileFids(fids, { jobLog, batchStart });

        const elapsedMs = Date.now() - batchStart;
        const elapsedSec = elapsedMs / 1000;
        const lastFid = fids[fids.length - 1];
        const avgTimePerFid = elapsedSec / fids.length;

        jobLog.info({
          event: "batch_done",
          fids_count: fids.length,
          fid_min: fidMin,
          fid_max: fidMax,
          dur_ms: elapsedMs,
          avg_sec_per_fid: Number(avgTimePerFid.toFixed(4)),
          t_since_proc_start_ms: sinceProcessStartMs(),
          t_since_proc_start_min: sinceProcessStartMin(),
        }, `Reconciled ${fids.length} FIDs (up to ${lastFid}) in ${elapsedSec.toFixed(2)}s (${avgTimePerFid.toFixed(2)}s/FID)`);
        
      } else if (job.name === "completionMarker") {
        const startedAt = new Date(job.data.startedAt as number);
        const duration = (Date.now() - startedAt.getTime()) / 1000 / 60;
        
        baseWorkerLog.info({ event: "run_completed", started_at: startedAt.toISOString(), finished_at: new Date().toISOString(), total_time_min: Number(duration.toFixed(2)) }, "Reconciliation completed");
        
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

  worker.on("active", (job) => {
    baseWorkerLog.debug({ event: "job_active", jobId: job.id, name: job.name }, `Job ${job.name} active`);
  });

  worker.on("completed", (job) => {
    baseWorkerLog.debug({ event: "job_completed", jobId: job.id, name: job.name }, `Job ${job.name} completed`);
  });

  worker.on("failed", (job, err) => {
    baseWorkerLog.error({ event: "job_failed", jobId: job?.id, name: job?.name, err: { message: err.message, stack: err.stack } }, `Job ${job?.name} failed`);
  });

  worker.on("stalled", (jobId) => {
    baseWorkerLog.warn({ event: "job_stalled", jobId }, `Job ${jobId} stalled`);
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