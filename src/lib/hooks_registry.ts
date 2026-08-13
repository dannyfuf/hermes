import type { HermesHooks, LoggerSink } from "./types.ts";

let _hooks: HermesHooks | null = null;
let _loggerSink: LoggerSink | null = null;

export function setHooks(hooks: HermesHooks): void {
  _hooks = hooks;
}

export function getHooks(): HermesHooks | null {
  return _hooks;
}

export function clearHooks(): void {
  _hooks = null;
}

export function setLoggerSink(sink: LoggerSink): void {
  _loggerSink = sink;
}

export function getLoggerSink(): LoggerSink | null {
  return _loggerSink;
}

export function clearLoggerSink(): void {
  _loggerSink = null;
}
