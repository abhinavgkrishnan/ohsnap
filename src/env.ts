// src/env.ts
import "dotenv/config";

export const COLORIZE =
  process.env["COLORIZE"] === "true" ? true : process.env["COLORIZE"] === "false" ? false : process.stdout.isTTY;
export const LOG_LEVEL = process.env["LOG_LEVEL"] || "info";

// Your Snapchain configuration
export const HUB_HOST = process.env["HUB_HOST"] || "localhost:3381";
export const HUB_SSL = process.env["HUB_SSL"] === "true" ? true : false;

// Database configuration
export const POSTGRES_URL = process.env["POSTGRES_URL"] || "postgres://shuttle:password@localhost:6541/shuttle";
export const POSTGRES_SCHEMA = process.env["POSTGRES_SCHEMA"] || "public";
export const REDIS_URL = process.env["REDIS_URL"] || "redis://localhost:16379";

// Based on your snapchain info: numShards: 2, shards: [0, 1, 2]
export const TOTAL_SHARDS = parseInt(process.env["SHARDS"] || "2");
export const SHARD_INDEX = parseInt(process.env["SHARD_NUM"] || "0");

// Backfill configuration
export const BACKFILL_FIDS = process.env["FIDS"] || "";
export const MAX_FID = process.env["MAX_FID"];

// Performance tuning
export const CONCURRENCY = parseInt(process.env["CONCURRENCY"] || "4");
export const USE_STREAMING_RPCS_FOR_BACKFILL = process.env["USE_STREAMING_RPCS_FOR_BACKFILL"] === "true" ? true : false;
export const SUBSCRIBE_RPC_TIMEOUT = parseInt(process.env["SUBSCRIBE_RPC_TIMEOUT"] || "300000");

// Monitoring
export const STATSD_HOST = process.env["STATSD_HOST"] || "localhost:8125";
export const STATSD_METRICS_PREFIX = process.env["STATSD_METRICS_PREFIX"] || "snapchain.shuttle.";