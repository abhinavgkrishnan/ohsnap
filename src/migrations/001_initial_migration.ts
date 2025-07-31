// src/migrations/001_initial_migration.ts
import { Kysely, sql } from "kysely";

// biome-ignore lint/suspicious/noExplicitAny: migration function needs any
export const up = async (db: Kysely<any>) => {
  // Create extension for ULID generation
  await sql`CREATE EXTENSION IF NOT EXISTS pgcrypto`.execute(db);

  // ULID generation function
  await sql`CREATE OR REPLACE FUNCTION generate_ulid() RETURNS uuid
    LANGUAGE sql STRICT PARALLEL SAFE
    RETURN ((lpad(to_hex((floor((EXTRACT(epoch FROM clock_timestamp()) * (1000)::numeric)))::bigint), 12, '0'::text) || encode(public.gen_random_bytes(10), 'hex'::text)))::uuid;
  `.execute(db);

  // MESSAGES table (core Farcaster messages)
  await db.schema
    .createTable("messages")
    .addColumn("id", "uuid", (col) => col.defaultTo(sql`generate_ulid()`))
    .addColumn("createdAt", "timestamptz", (col) => col.notNull().defaultTo(sql`current_timestamp`))
    .addColumn("updatedAt", "timestamptz", (col) => col.notNull().defaultTo(sql`current_timestamp`))
    .addColumn("timestamp", "timestamptz", (col) => col.notNull())
    .addColumn("deletedAt", "timestamptz")
    .addColumn("prunedAt", "timestamptz")
    .addColumn("revokedAt", "timestamptz")
    .addColumn("fid", "bigint", (col) => col.notNull())
    .addColumn("type", sql`smallint`, (col) => col.notNull())
    .addColumn("hashScheme", sql`smallint`, (col) => col.notNull())
    .addColumn("signatureScheme", sql`smallint`, (col) => col.notNull())
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