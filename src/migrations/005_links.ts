import { Kysely, sql } from "kysely";

// biome-ignore lint/suspicious/noExplicitAny: migration function needs any
export const up = async (db: Kysely<any>) => {
  // Links table
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
    .execute();

  await db.schema
    .createIndex("links_fid_timestamp_index")
    .on("links")
    .columns(["fid", "timestamp"])
    .where(sql.ref("deleted_at"), "is", null)
    .execute();

  await db.schema
    .createIndex("links_type_index")
    .on("links")
    .columns(["type", "fid"])
    .where(sql.ref("deleted_at"), "is", null)
    .execute();
};