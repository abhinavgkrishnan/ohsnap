import { Kysely, sql } from "kysely";

// biome-ignore lint/suspicious/noExplicitAny: migration function needs any
export const up = async (db: Kysely<any>) => {
  // Reactions table
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
    .execute();

  await db.schema
    .createIndex("reactions_fid_timestamp_index")
    .on("reactions")
    .columns(["fid", "timestamp"])
    .where(sql.ref("deleted_at"), "is", null)
    .execute();

  await db.schema
    .createIndex("reactions_target_index")
    .on("reactions")
    .columns(["targetFid", "targetHash"])
    .where(sql.ref("deleted_at"), "is", null)
    .execute();
};