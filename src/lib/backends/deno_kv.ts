import type { BackendAdapter, EnqueueOptions } from "../backend.ts";
import type { JobPayload } from "../types.ts";

export interface DenoKvBackendOptions {
  path?: string;
}

class TDenoKvBackend implements BackendAdapter {
  private kv: Deno.Kv | null = null;
  private path?: string;

  constructor(options?: DenoKvBackendOptions) {
    this.path = options?.path;
  }

  private async getKv(): Promise<Deno.Kv> {
    if (!this.kv) {
      this.kv = await Deno.openKv(this.path);
    }
    return this.kv;
  }

  async enqueue(payload: JobPayload, options?: EnqueueOptions): Promise<void> {
    const kv = await this.getKv();
    await kv.enqueue(payload, { delay: options?.delay });
  }

  async listen(
    handler: (payload: JobPayload) => Promise<void>,
    _options?: { queueNames?: string[] },
  ): Promise<void> {
    const kv = await this.getKv();
    kv.listenQueue(async (message: unknown) => {
      await handler(message as JobPayload);
    });
  }

  close(): Promise<void> {
    if (this.kv) {
      this.kv.close();
      this.kv = null;
    }
    return Promise.resolve();
  }
}

export const DenoKvBackend = (options?: DenoKvBackendOptions): BackendAdapter => {
  return new TDenoKvBackend(options);
};
