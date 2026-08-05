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

  return { value, unit };
}

/** Convert an {@link EveryInterval} to milliseconds. */
export function intervalToMs(interval: EveryInterval): number {
  const milliseconds = interval.value * MS_PER_UNIT[interval.unit];
  if (!Number.isSafeInteger(milliseconds)) {
    throw new Error(
      `Every interval "${interval.value}${interval.unit}" exceeds the maximum safe millisecond value.`,
    );
  }
  return milliseconds;
}

/**
 * Convert an EveryInterval to a Deno.cron CronSchedule object.
 * Converts exact multiples to a coarser unit when Deno.cron cannot represent
 * the original amount and throws for arbitrary unsupported intervals.
 *
 * Examples:
 *   "5m"  → { minute: { every: 5 } }
 *   "2h"  → { hour: { every: 2 } }
 *   "1d"  → { dayOfMonth: { every: 1 } }
 */
export function intervalToCronSchedule(
  interval: EveryInterval,
): Record<string, { every: number }> {
  let { value, unit } = interval;

  if (unit === "s" && value % 60 === 0) {
    value /= 60;
    unit = "m";
  }
  if (unit === "m" && value > 59 && value % 60 === 0) {
    value /= 60;
    unit = "h";
  }
  if (unit === "h" && value > 23 && value % 24 === 0) {
    value /= 24;
    unit = "d";
  }

  const withinDenoCronRange = (unit === "m" && value <= 59) ||
    (unit === "h" && value <= 23) ||
    (unit === "d" && value <= 31);
  if (!withinDenoCronRange) {
    throw new Error(
      `Deno KV recurrence supports 1-59 minute, 1-23 hour, and 1-31 day intervals, ` +
        `with exact unit conversions; cannot represent "${interval.value}${interval.unit}". ` +
        `Use the BullMQ backend for arbitrary intervals.`,
    );
  }

  const mapping: Record<string, string> = {
    m: "minute",
    h: "hour",
    d: "dayOfMonth",
  };
  return { [mapping[unit]]: { every: value } };
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
