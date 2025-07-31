// src/migrations/002_custom_tables.ts
import { Kysely, sql } from "kysely";

// biome-ignore lint/suspicious/noExplicitAny: migration function needs any
export const up = async (db: Kysely<any>) => {
  // CASTS table - materialized view of cast messages
  await db.schema
    .createTable("casts")
    .addColumn("id", "uuid", (col) => col.defaultTo(sql`generate_ulid()`))
    .addColumn("createdAt", "timestamptz", (col) => col.notNull().defaultTo(sql`current_timestamp`))
    .addColumn("updatedAt", "timestamptz", (col) => col.notNull().defaultTo(sql`current_timestamp`))
    .addColumn("deletedAt", "timestamptz")
    .addColumn("timestamp", "timestamptz", (col) => col.notNull())
    .addColumn("fid", "bigint", (col) => col.notNull())
    .addColumn("hash", "bytea", (col) => col.notNull())
    .addColumn("text", "text", (col) => col.notNull())
    .addColumn("parentFid", "bigint")
    .addColumn("parentHash", "bytea")
    .addColumn("embedCount", "integer", (col) => col.notNull().defaultTo(0))
    .addColumn("mentionCount", "integer", (col) => col.notNull().defaultTo(0))
    .addPrimaryKeyConstraint("casts_pkey", ["id"])
    .addUniqueConstraint("casts_hash_unique", ["hash"])
    .execute();

  // REACTIONS table - materialized view of reactions
  await db.schema
    .createTable("reactions")
    .addColumn("id", "uuid", (col) => col.defaultTo(sql`generate_ulid()`))
    .addColumn("createdAt", "timestamptz", (col) => col.notNull().defaultTo(sql`current_timestamp`))
    .addColumn("updatedAt", "timestamptz", (col) => col.notNull().defaultTo(sql`current_timestamp`))
    .addColumn("deletedAt", "timestamptz")
    .addColumn("timestamp", "timestamptz", (col) => col.notNull())
    .addColumn("fid", "bigint", (col) => col.notNull())
    .addColumn("hash", "bytea", (col) => col.notNull())
    .addColumn("type", "integer", (col) => col.notNull())
    .addColumn("targetFid", "bigint")
    .addColumn("targetHash", "bytea")
    .addPrimaryKeyConstraint("reactions_pkey", ["id"])
    .addUniqueConstraint("reactions_hash_unique", ["hash"])
    .execute();

  // LINKS table - materialized view of follow/unfollow
  await db.schema
    .createTable("links")
    .addColumn("id", "uuid", (col) => col.defaultTo(sql`generate_ulid()`))
    .addColumn("createdAt", "timestamptz", (col) => col.notNull().defaultTo(sql`current_timestamp`))
    .addColumn("updatedAt", "timestamptz", (col) => col.notNull().defaultTo(sql`current_timestamp`))
    .addColumn("deletedAt", "timestamptz")
    .addColumn("timestamp", "timestamptz", (col) => col.notNull())
    .addColumn("fid", "bigint", (col) => col.notNull())
    .addColumn("hash", "bytea", (col) => col.notNull())
    .addColumn("type", "text", (col) => col.notNull())
    .addColumn("targetFid", "bigint")
    .addPrimaryKeyConstraint("links_pkey", ["id"])
    .addUniqueConstraint("links_hash_unique", ["hash"])
    .execute();

  // ON-CHAIN EVENTS table
  await db.schema
    .createTable("onchain_events")
    .addColumn("id", "uuid", (col) => col.defaultTo(sql`generate_ulid()`))
    .addColumn("createdAt", "timestamptz", (col) => col.notNull().defaultTo(sql`current_timestamp`))
    .addColumn("updatedAt", "timestamptz", (col) => col.notNull().defaultTo(sql`current_timestamp`))
    .addColumn("timestamp", "timestamptz", (col) => col.notNull())
    .addColumn("fid", "bigint", (col) => col.notNull())
    .addColumn("blockNumber", "bigint", (col) => col.notNull())
    .addColumn("logIndex", "integer", (col) => col.notNull())
    .addColumn("type", "integer", (col) => col.notNull())
    .addColumn("txHash", "bytea", (col) => col.notNull())
    .addColumn("body", "json", (col) => col.notNull())
    .addPrimaryKeyConstraint("onchain_events_pkey", ["id"])
    .execute();

  // MESSAGE STATS table for analytics
  await db.schema
    .createTable("message_stats")
    .addColumn("id", "uuid", (col) => col.defaultTo(sql`generate_ulid()`))
    .addColumn("createdAt", "timestamptz", (col) => col.notNull().defaultTo(sql`current_timestamp`))
    .addColumn("messageType", "integer", (col) => col.notNull())
    .addColumn("fid", "bigint", (col) => col.notNull())
    .addColumn("operation", "text", (col) => col.notNull())
    .addColumn("state", "text", (col) => col.notNull())
    .addColumn("timestamp", "timestamptz", (col) => col.notNull())
    .addColumn("blockNumber", "bigint", (col) => col.notNull())
    .addColumn("wasMissed", "boolean", (col) => col.notNull().defaultTo(false))
    .addPrimaryKeyConstraint("message_stats_pkey", ["id"])
    .execute();

  // Performance indexes
  await db.schema.createIndex("casts_fid_timestamp_index")
    .on("casts").columns(["fid", "timestamp"])
    .where(sql.ref("deletedAt"), "is", null).execute();
    
  await db.schema.createIndex("casts_parent_index")
    .on("casts").columns(["parentFid", "parentHash"])
    .where(sql.ref("deletedAt"), "is", null).execute();

  await db.schema.createIndex("reactions_fid_timestamp_index")
    .on("reactions").columns(["fid", "timestamp"])
    .where(sql.ref("deletedAt"), "is", null).execute();
    
  await db.schema.createIndex("reactions_target_index")
    .on("reactions").columns(["targetFid", "targetHash"])
    .where(sql.ref("deletedAt"), "is", null).execute();

  await db.schema.createIndex("links_fid_target_index")
    .on("links").columns(["fid", "targetFid", "type"])
    .where(sql.ref("deletedAt"), "is", null).execute();

  await db.schema.createIndex("onchain_events_block_log_index")
    .on("onchain_events").columns(["blockNumber", "logIndex"]).execute();
    
  await db.schema.createIndex("onchain_events_fid_type_index")
    .on("onchain_events").columns(["fid", "type"]).execute();

  await db.schema.createIndex("message_stats_type_timestamp_index")
    .on("message_stats").columns(["messageType", "timestamp"]).execute();
    
  await db.schema.createIndex("message_stats_fid_timestamp_index")
    .on("message_stats").columns(["fid", "timestamp"]).execute();
};