export type RedisTestConfig = {
  host: string;
  port: number;
  password?: string;
};

export async function loadEnvLocal(
  path = ".env.local",
): Promise<Record<string, string>> {
  const fileValues: Record<string, string> = {};

  try {
    const contents = await Deno.readTextFile(path);
    for (const rawLine of contents.split(/\r?\n/)) {
      const line = rawLine.trim();
      if (line === "" || line.startsWith("#")) continue;

      const separatorIndex = line.indexOf("=");
      if (separatorIndex < 1) continue;

      const key = line.slice(0, separatorIndex).trim();
      const value = line.slice(separatorIndex + 1).trim();
      fileValues[key] = value;
    }
  } catch (error) {
    if (!(error instanceof Deno.errors.NotFound)) throw error;
  }

  return { ...fileValues, ...Deno.env.toObject() };
}

export async function redisTestConfig(): Promise<RedisTestConfig> {
  const env = await loadEnvLocal();
  const host = env.REDIS_HOST ?? "127.0.0.1";
  const port = Number(env.REDIS_PORT ?? "6379");

  if (!Number.isInteger(port) || port < 1 || port > 65_535) {
    throw new Error(`Invalid REDIS_PORT: ${env.REDIS_PORT}`);
  }

  return {
    host,
    port,
    ...(env.REDIS_PASSWORD ? { password: env.REDIS_PASSWORD } : {}),
  };
}
