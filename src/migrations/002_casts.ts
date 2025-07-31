import { Kysely, sql } from "kysely";

// biome-ignore lint/suspicious/noExplicitAny: legacy code, avoid using ignore for new code
export const up = async (db: Kysely<any>) => {
  // Casts - COMPLETE VERSION with all columns your app needs
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
    // ADD THE MISSING COLUMNS:
    .addColumn("parentFid", "bigint")           // This was missing!
    .addColumn("parentHash", "bytea")           // This was missing!
    .addColumn("embedCount", "integer", (col) => col.notNull().defaultTo(0))    // This was missing!
    .addColumn("mentionCount", "integer", (col) => col.notNull().defaultTo(0))  // This was missing!
    .execute();

  await db.schema
    .createIndex("casts_fid_timestamp_index")
    .on("casts")
    .columns(["fid", "timestamp"])
    .where(sql.ref("deletedAt"), "is", null)
    .execute();

  await db.schema
    .createIndex("casts_parent_index")
    .on("casts")
    .columns(["parentFid", "parentHash"])
    .where(sql.ref("deletedAt"), "is", null)
    .execute();
};