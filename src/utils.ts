// src/utils.ts
import { fromFarcasterTime } from "@farcaster/hub-nodejs";

export function bytesToHex(value: Uint8Array): `0x${string}` {
  return `0x${Buffer.from(value).toString("hex")}`;
}

export function farcasterTimeToDate(time: number): Date;
export function farcasterTimeToDate(time: null): null;
export function farcasterTimeToDate(time: undefined): undefined;
export function farcasterTimeToDate(time: number | null | undefined): Date | null | undefined {
  if (time === undefined) return undefined;
  if (time === null) return null;
  const result = fromFarcasterTime(time);
  if (result.isErr()) throw result.error;
  return new Date(result.value);
}

export function sleep(ms: number) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

export function formatBytes(bytes: number): string {
  const sizes = ['Bytes', 'KB', 'MB', 'GB'];
  if (bytes === 0) return '0 Bytes';
  const i = Math.floor(Math.log(bytes) / Math.log(1024));
  return Math.round(bytes / Math.pow(1024, i) * 100) / 100 + ' ' + sizes[i];
}

export function formatDuration(ms: number): string {
  const seconds = Math.floor(ms / 1000);
  const minutes = Math.floor(seconds / 60);
  const hours = Math.floor(minutes / 60);
  
  if (hours > 0) {
    return `${hours}h ${minutes % 60}m ${seconds % 60}s`;
  } else if (minutes > 0) {
    return `${minutes}m ${seconds % 60}s`;
  } else {
    return `${seconds}s`;
  }
}

// Helper to create readable message type names
export function getMessageTypeName(type: number): string {
  const types: Record<number, string> = {
    1: "CAST_ADD",
    2: "CAST_REMOVE", 
    3: "REACTION_ADD",
    4: "REACTION_REMOVE",
    5: "LINK_ADD",
    6: "LINK_REMOVE",
    7: "VERIFICATION_ADD_ETH_ADDRESS",
    8: "VERIFICATION_REMOVE",
    9: "SIGNER_ADD",
    10: "SIGNER_REMOVE",
    11: "USER_DATA_ADD",
    12: "USERNAME_PROOF",
    13: "LINK_COMPACT_STATE",
  };
  return types[type] || `UNKNOWN_${type}`;
}