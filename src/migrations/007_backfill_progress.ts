import { Kysely, sql } from "kysely";

// biome-ignore lint/suspicious/noExplicitAny: migration function needs any
export const up = async (db: Kysely<any>) => {
  // Backfill progress tracking table
  await db.schema
    .createTable("backfill_progress")
    .addColumn("id", "uuid", (col) => col.defaultTo(sql`generate_ulid()`))
    .addColumn("createdAt", "timestamptz", (col) => col.notNull().defaultTo(sql`current_timestamp`))
    .addColumn("updatedAt", "timestamptz", (col) => col.notNull().defaultTo(sql`current_timestamp`))
    .addColumn("batchId", "text", (col) => col.notNull())
    .addColumn("fid", "bigint", (col) => col.notNull())
    .addColumn("status", "text", (col) => col.notNull()) // "pending", "completed", "failed"
    .addColumn("startedAt", "timestamptz")
    .addColumn("completedAt", "timestamptz")
    .addColumn("error", "text")
    .addColumn("messagesProcessed", "integer", (col) => col.notNull().defaultTo(0))
    .addColumn("processingTimeSeconds", "numeric")
    .execute();

  await db.schema
    .createIndex("backfill_progress_batch_fid_index")
    .on("backfill_progress")
    .columns(["batchId", "fid"])
    .execute();

  await db.schema
    .createIndex("backfill_progress_status_index")
    .on("backfill_progress")
    .columns(["status"])
    .execute();

  await db.schema
    .createIndex("backfill_progress_batch_status_index")
    .on("backfill_progress")
    .columns(["batchId", "status"])
    .execute();
};

