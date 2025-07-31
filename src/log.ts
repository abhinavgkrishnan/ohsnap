// src/log.ts
import { pino } from "pino";
import { COLORIZE, LOG_LEVEL } from "./env";

export const log = pino({
  level: LOG_LEVEL,
  transport: {
    target: "pino-pretty",
    options: {
      colorize: COLORIZE,
      singleLine: true,
      translateTime: "SYS:standard",
      ignore: "pid,hostname",
    },
  },
});

export type Logger = pino.Logger;