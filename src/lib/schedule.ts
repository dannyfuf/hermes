/** Supported time units for recurring job intervals. */
export type TimeUnit = "s" | "m" | "h" | "d";

/** A parsed interval with a numeric value and time unit. */
export interface EveryInterval {
  value: number;
  unit: TimeUnit;
}

/** A recurring schedule defined by either an interval string or a cron expression. */
export interface RecurringSchedule {
  every?: string;
  cron?: string;
}

const EVERY_PATTERN = /^(\d+)(s|m|h|d)$/;

const MS_PER_UNIT: Record<TimeUnit, number> = {
  s: 1_000,
  m: 60_000,
  h: 3_600_000,
  d: 86_400_000,
};

const MAX_VALUES: Record<TimeUnit, number> = {
  s: 59,
  m: 59,
  h: 23,
  d: 365,
};

/** Parse an interval string like "5m" or "24h" into an {@link EveryInterval}. */
export function parseEveryInterval(str: string): EveryInterval {
  const match = str.match(EVERY_PATTERN);
  if (!match) {
    throw new Error(
      `Invalid every interval "${str}". Expected format: [number][unit] where unit is s, m, h, or d (e.g., "5s", "10m", "24h", "7d").`,
    );
  }

  const value = parseInt(match[1], 10);
  const unit = match[2] as TimeUnit;

  if (value <= 0) {
    throw new Error(
      `Invalid every interval "${str}". Value must be a positive integer.`,
    );
  }

  if (value > MAX_VALUES[unit]) {
    throw new Error(
      `Invalid every interval "${str}". Maximum value for "${unit}" is ${MAX_VALUES[unit]}.`,
    );
  }

  return { value, unit };
}

/** Convert an {@link EveryInterval} to milliseconds. */
export function intervalToMs(interval: EveryInterval): number {
  return interval.value * MS_PER_UNIT[interval.unit];
}

/**
 * Convert an EveryInterval to a Deno.cron CronSchedule object.
 * Throws if unit is 's' (seconds not supported by Deno.cron).
 *
 * Examples:
 *   "5m"  → { minute: { every: 5 } }
 *   "2h"  → { hour: { every: 2 } }
 *   "1d"  → { day: { every: 1 } }
 */
export function intervalToCronSchedule(
  interval: EveryInterval,
): Record<string, { every: number }> {
  if (interval.unit === "s") {
    throw new Error(
      `Seconds-level intervals are not supported on the Deno KV backend. ` +
        `Minimum granularity is 1 minute (e.g., "1m"). Use the BullMQ backend for sub-minute intervals.`,
    );
  }

  const mapping: Record<string, string> = { m: "minute", h: "hour", d: "day" };
  const field = mapping[interval.unit];
  return { [field]: { every: interval.value } };
}

const CRON_FIELD_COUNT_MIN = 5;
const CRON_FIELD_COUNT_MAX = 6;

/** Validate that a string is a well-formed cron expression (5 or 6 fields). */
export function validateCronExpression(cron: string): boolean {
  const parts = cron.trim().split(/\s+/);
  if (
    parts.length < CRON_FIELD_COUNT_MIN ||
    parts.length > CRON_FIELD_COUNT_MAX
  ) {
    return false;
  }

  const cronPattern = /^[\d*,/\-]+$/;
  return parts.every((part) => cronPattern.test(part));
}
