// src/log.ts
import { pino } from "pino";
import os from "node:os";
import { randomUUID } from "node:crypto";
import { COLORIZE, LOG_LEVEL, SHARD_INDEX, TOTAL_SHARDS, POSTGRES_SCHEMA, HUB_HOST, USE_STREAMING_RPCS_FOR_BACKFILL, SUBSCRIBE_RPC_TIMEOUT } from "./env";

export type Logger = pino.Logger;

export const processStartMs = Date.now();
export const runId = randomUUID();

export function sinceProcessStartMs(): number {
  return Date.now() - processStartMs;
}

export function sinceProcessStartMin(): number {
  return sinceProcessStartMs() / 1000 / 60;
}

export const log = pino({
  level: LOG_LEVEL,
  base: {
    runId,
    pid: process.pid,
    hostname: os.hostname(),
    shardIndex: SHARD_INDEX,
    totalShards: TOTAL_SHARDS,
    dbSchema: POSTGRES_SCHEMA,
    hubHost: HUB_HOST,
    useStreamingRpcs: USE_STREAMING_RPCS_FOR_BACKFILL,
    subscribeRpcTimeoutMs: SUBSCRIBE_RPC_TIMEOUT,
  },
  transport: {
    target: "pino-pretty",
    options: {
      colorize: COLORIZE,
      singleLine: true,
      translateTime: "SYS:standard",
      ignore: "pid,hostname",
    },
  },
});

export function childLogger(context: Record<string, unknown>): Logger {
  return log.child(context);
}

export function nowTs() {
  const tSinceProcMs = sinceProcessStartMs();
  return {
    t_since_proc_start_ms: tSinceProcMs,
    t_since_proc_start_min: Math.round((tSinceProcMs / 1000 / 60) * 100) / 100,
  };
}

export function withTimingFields(extra?: Record<string, unknown>) {
  return { ...nowTs(), ...(extra || {}) };
}