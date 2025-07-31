// src/db.ts
import { ColumnType, FileMigrationProvider, Generated, Kysely, Migrator } from "kysely";
import { Logger } from "./log";
import { err, ok, Result } from "neverthrow";
import path from "path";
import { promises as fs } from "fs";
import { fileURLToPath } from "node:url";
import { HubTables } from "@farcaster/shuttle";
import { Fid } from "@farcaster/shuttle";

const createMigrator = async (db: Kysely<HubTables>, dbSchema: string, log: Logger) => {
  const currentDir = path.dirname(fileURLToPath(import.meta.url));
  const migrator = new Migrator({
    db,
    migrationTableSchema: dbSchema,
    provider: new FileMigrationProvider({
      fs,
      path,
      migrationFolder: path.join(currentDir, "migrations"),
    }),
  });

  return migrator;
};

export const migrateToLatest = async (
  db: Kysely<HubTables>,
  dbSchema: string,
  log: Logger,
): Promise<Result<void, unknown>> => {
  const migrator = await createMigrator(db, dbSchema, log);

  const { error, results } = await migrator.migrateToLatest();

  results?.forEach((it) => {
    if (it.status === "Success") {
      log.info(`✅ Migration "${it.migrationName}" executed successfully`);
    } else if (it.status === "Error") {
      log.error(`❌ Failed to execute migration "${it.migrationName}"`);
    }
  });

  if (error) {
    log.error("❌ Failed to apply all database migrations");
    log.error(error);
    return err(error);
  }

  log.info("✅ All migrations up to date");
  return ok(undefined);
};

// Custom table definitions for your Snapchain data
export type CastRow = {
  id: Generated<string>;
  createdAt: Generated<Date>;
  updatedAt: Generated<Date>;
  deletedAt: Date | null;
  timestamp: Date;
  fid: Fid;
  hash: Uint8Array;
  text: string;
  parentFid: Fid | null;
  parentHash: Uint8Array | null;
  embedCount: number;
  mentionCount: number;
};

export type ReactionRow = {
  id: Generated<string>;
  createdAt: Generated<Date>;
  updatedAt: Generated<Date>;
  deletedAt: Date | null;
  timestamp: Date;
  fid: Fid;
  hash: Uint8Array;
  type: number; // ReactionType enum
  targetFid: Fid | null;
  targetHash: Uint8Array | null;
};

export type LinkRow = {
  id: Generated<string>;
  createdAt: Generated<Date>;
  updatedAt: Generated<Date>;
  deletedAt: Date | null;
  timestamp: Date;
  fid: Fid;
  hash: Uint8Array;
  type: string; // "follow", etc.
  targetFid: Fid | null;
};

export type OnChainEventRow = {
  id: Generated<string>;
  createdAt: Generated<Date>;
  updatedAt: Generated<Date>;
  timestamp: Date;
  fid: Fid;
  blockNumber: number;
  logIndex: number;
  type: number;
  txHash: Uint8Array;
  body: Record<string, string | number>;
};

export type MessageStatsRow = {
  id: Generated<string>;
  createdAt: Generated<Date>;
  messageType: number;
  fid: Fid;
  operation: string; // "merge", "delete", "revoke", "prune"
  state: string; // "created", "deleted"
  timestamp: Date;
  blockNumber: number;
  wasMissed: boolean;
};

// Extended interface that includes your custom tables
export interface Tables extends HubTables {
  casts: CastRow;
  reactions: ReactionRow;
  links: LinkRow;
  onchain_events: OnChainEventRow;
  message_stats: MessageStatsRow;
}

export type CustomDb = Kysely<Tables>;

// src/migrations/001_initial_migration.ts
export const migration_001 = `
import { Kysely, sql } from "kysely";

export const up = async (db: Kysely<any>) => {
  // Create extension for ULID generation
  await sql\`CREATE EXTENSION IF NOT EXISTS pgcrypto\`.execute(db);

  // ULID generation function
  await sql\`CREATE OR REPLACE FUNCTION generate_ulid() RETURNS uuid
    LANGUAGE sql STRICT PARALLEL SAFE
    RETURN ((lpad(to_hex((floor((EXTRACT(epoch FROM clock_timestamp()) * (1000)::numeric)))::bigint), 12, '0'::text) || encode(public.gen_random_bytes(10), 'hex'::text)))::uuid;
  \`.execute(db);

  // MESSAGES table (core Farcaster messages)
  await db.schema
    .createTable("messages")
    .addColumn("id", "uuid", (col) => col.defaultTo(sql\`generate_ulid()\`))
    .addColumn("createdAt", "timestamptz", (col) => col.notNull().defaultTo(sql\`current_timestamp\`))
    .addColumn("updatedAt", "timestamptz", (col) => col.notNull().defaultTo(sql\`current_timestamp\`))
    .addColumn("timestamp", "timestamptz", (col) => col.notNull())
    .addColumn("deletedAt", "timestamptz")
    .addColumn("prunedAt", "timestamptz")
    .addColumn("revokedAt", "timestamptz")
    .addColumn("fid", "bigint", (col) => col.notNull())
    .addColumn("type", sql\`smallint\`, (col) => col.notNull())
    .addColumn("hashScheme", sql\`smallint\`, (col) => col.notNull())
    .addColumn("signatureScheme", sql\`smallint\`, (col) => col.notNull())
    .addColumn("hash", "bytea", (col) => col.notNull())
    .addColumn("signer", "bytea", (col) => col.notNull())
    .addColumn("body", "json", (col) => col.notNull())
    .addColumn("raw", "bytea", (col) => col.notNull())
    .addUniqueConstraint("messages_hash_unique", ["hash"])
    .addPrimaryKeyConstraint("messages_pkey", ["id"])
    .execute();

  // Indexes for messages table
  await db.schema.createIndex("messages_timestamp_index").on("messages").columns(["timestamp"]).execute();
  await db.schema.createIndex("messages_fid_index").on("messages").columns(["fid"]).execute();
  await db.schema.createIndex("messages_type_index").on("messages").columns(["type"]).execute();
  await db.schema.createIndex("messages_signer_index").on("messages").columns(["signer"]).execute();
};
`;

// src/migrations/002_custom_tables.ts
export const migration_002 = `
import { Kysely, sql } from "kysely";

export const up = async (db: Kysely<any>) => {
  // CASTS table - materialized view of cast messages
  await db.schema
    .createTable("casts")
    .addColumn("id", "uuid", (col) => col.defaultTo(sql\`generate_ulid()\`))
    .addColumn("createdAt", "timestamptz", (col) => col.notNull().defaultTo(sql\`current_timestamp\`))
    .addColumn("updatedAt", "timestamptz", (col) => col.notNull().defaultTo(sql\`current_timestamp\`))
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
    .addColumn("id", "uuid", (col) => col.defaultTo(sql\`generate_ulid()\`))
    .addColumn("createdAt", "timestamptz", (col) => col.notNull().defaultTo(sql\`current_timestamp\`))
    .addColumn("updatedAt", "timestamptz", (col) => col.notNull().defaultTo(sql\`current_timestamp\`))
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
    .addColumn("id", "uuid", (col) => col.defaultTo(sql\`generate_ulid()\`))
    .addColumn("createdAt", "timestamptz", (col) => col.notNull().defaultTo(sql\`current_timestamp\`))
    .addColumn("updatedAt", "timestamptz", (col) => col.notNull().defaultTo(sql\`current_timestamp\`))
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
    .addColumn("id", "uuid", (col) => col.defaultTo(sql\`generate_ulid()\`))
    .addColumn("createdAt", "timestamptz", (col) => col.notNull().defaultTo(sql\`current_timestamp\`))
    .addColumn("updatedAt", "timestamptz", (col) => col.notNull().defaultTo(sql\`current_timestamp\`))
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
    .addColumn("id", "uuid", (col) => col.defaultTo(sql\`generate_ulid()\`))
    .addColumn("createdAt", "timestamptz", (col) => col.notNull().defaultTo(sql\`current_timestamp\`))
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
    .where(sql.ref("deleted_at"), "is", null).execute();
    
  await db.schema.createIndex("casts_parent_index")
    .on("casts").columns(["parentFid", "parentHash"])
    .where(sql.ref("deleted_at"), "is", null).execute();

  await db.schema.createIndex("reactions_fid_timestamp_index")
    .on("reactions").columns(["fid", "timestamp"])
    .where(sql.ref("deleted_at"), "is", null).execute();
    
  await db.schema.createIndex("reactions_target_index")
    .on("reactions").columns(["targetFid", "targetHash"])
    .where(sql.ref("deleted_at"), "is", null).execute();

  await db.schema.createIndex("links_fid_target_index")
    .on("links").columns(["fid", "targetFid", "type"])
    .where(sql.ref("deleted_at"), "is", null).execute();

  await db.schema.createIndex("onchain_events_block_log_index")
    .on("onchain_events").columns(["blockNumber", "logIndex"]).execute();
    
  await db.schema.createIndex("onchain_events_fid_type_index")
    .on("onchain_events").columns(["fid", "type"]).execute();

  await db.schema.createIndex("message_stats_type_timestamp_index")
    .on("message_stats").columns(["messageType", "timestamp"]).execute();
    
  await db.schema.createIndex("message_stats_fid_timestamp_index")
    .on("message_stats").columns(["fid", "timestamp"]).execute();
};
`;

// Create actual migration files
export const migrations = {
  "001_initial_migration.ts": migration_001,
  "002_custom_tables.ts": migration_002,
};