import type { BackendAdapter } from "./backend.ts";

let _backend: BackendAdapter | null = null;

export function setBackend(backend: BackendAdapter): void {
  _backend = backend;
}

export function getBackend(): BackendAdapter {
  if (!_backend) {
    throw new Error(
      "No backend configured. Call Hermes().start() or configure() before enqueuing jobs.",
    );
  }
  return _backend;
}

export function clearBackend(): void {
  _backend = null;
}
