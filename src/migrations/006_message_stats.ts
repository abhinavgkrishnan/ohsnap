import { Kysely, sql } from "kysely";

// biome-ignore lint/suspicious/noExplicitAny: migration function needs any
export const up = async (db: Kysely<any>) => {
  // Message stats table
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
    .execute();

  await db.schema
    .createIndex("message_stats_type_timestamp_index")
    .on("message_stats")
    .columns(["messageType", "timestamp"])
    .execute();

  await db.schema
    .createIndex("message_stats_fid_index")
    .on("message_stats")
    .columns(["fid"])
    .execute();
};