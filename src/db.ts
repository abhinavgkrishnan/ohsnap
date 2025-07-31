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