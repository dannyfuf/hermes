import { assertEquals, assertThrows } from "@std/assert";
import {
  intervalToCronSchedule,
  intervalToMs,
  parseEveryInterval,
  validateCronExpression,
} from "../schedule.ts";

Deno.test("parseEveryInterval", async (t) => {
  await t.step("parses valid second intervals", () => {
    assertEquals(parseEveryInterval("1s"), { value: 1, unit: "s" });
    assertEquals(parseEveryInterval("30s"), { value: 30, unit: "s" });
    assertEquals(parseEveryInterval("59s"), { value: 59, unit: "s" });
  });

  await t.step("parses valid minute intervals", () => {
    assertEquals(parseEveryInterval("1m"), { value: 1, unit: "m" });
    assertEquals(parseEveryInterval("5m"), { value: 5, unit: "m" });
    assertEquals(parseEveryInterval("59m"), { value: 59, unit: "m" });
  });

  await t.step("parses valid hour intervals", () => {
    assertEquals(parseEveryInterval("1h"), { value: 1, unit: "h" });
    assertEquals(parseEveryInterval("12h"), { value: 12, unit: "h" });
    assertEquals(parseEveryInterval("23h"), { value: 23, unit: "h" });
  });

  await t.step("parses valid day intervals", () => {
    assertEquals(parseEveryInterval("1d"), { value: 1, unit: "d" });
    assertEquals(parseEveryInterval("7d"), { value: 7, unit: "d" });
    assertEquals(parseEveryInterval("365d"), { value: 365, unit: "d" });
  });

  await t.step("throws on invalid format", () => {
    assertThrows(
      () => parseEveryInterval("5x"),
      Error,
      "Invalid every interval",
    );
    assertThrows(
      () => parseEveryInterval("abc"),
      Error,
      "Invalid every interval",
    );
    assertThrows(
      () => parseEveryInterval("5ss"),
      Error,
      "Invalid every interval",
    );
    assertThrows(() => parseEveryInterval(""), Error, "Invalid every interval");
    assertThrows(
      () => parseEveryInterval("m5"),
      Error,
      "Invalid every interval",
    );
    assertThrows(
      () => parseEveryInterval("-5s"),
      Error,
      "Invalid every interval",
    );
  });

  await t.step("throws on zero value", () => {
    assertThrows(() => parseEveryInterval("0s"), Error, "positive integer");
  });

  await t.step("accepts uncapped positive amounts", () => {
    assertEquals(parseEveryInterval("60s"), { value: 60, unit: "s" });
    assertEquals(parseEveryInterval("60m"), { value: 60, unit: "m" });
    assertEquals(parseEveryInterval("24h"), { value: 24, unit: "h" });
    assertEquals(parseEveryInterval("366d"), { value: 366, unit: "d" });
    assertEquals(
      intervalToMs(parseEveryInterval("120s")),
      120_000,
    );
  });
});

Deno.test("intervalToMs", async (t) => {
  await t.step("converts seconds to ms", () => {
    assertEquals(intervalToMs({ value: 5, unit: "s" }), 5_000);
    assertEquals(intervalToMs({ value: 30, unit: "s" }), 30_000);
  });

  await t.step("converts minutes to ms", () => {
    assertEquals(intervalToMs({ value: 1, unit: "m" }), 60_000);
    assertEquals(intervalToMs({ value: 10, unit: "m" }), 600_000);
  });

  await t.step("converts hours to ms", () => {
    assertEquals(intervalToMs({ value: 1, unit: "h" }), 3_600_000);
    assertEquals(intervalToMs({ value: 24, unit: "h" }), 86_400_000);
  });

  await t.step("converts days to ms", () => {
    assertEquals(intervalToMs({ value: 1, unit: "d" }), 86_400_000);
    assertEquals(intervalToMs({ value: 7, unit: "d" }), 604_800_000);
  });

  await t.step("rejects intervals that overflow safe integers", () => {
    assertThrows(
      () => intervalToMs({ value: Number.MAX_SAFE_INTEGER, unit: "d" }),
      Error,
      "exceeds the maximum safe millisecond value",
    );
  });
});

Deno.test("intervalToCronSchedule", async (t) => {
  await t.step("converts minutes to cron schedule", () => {
    assertEquals(intervalToCronSchedule({ value: 5, unit: "m" }), {
      minute: { every: 5 },
    });
  });

  await t.step("converts hours to cron schedule", () => {
    assertEquals(intervalToCronSchedule({ value: 2, unit: "h" }), {
      hour: { every: 2 },
    });
  });

  await t.step("converts days to cron schedule", () => {
    assertEquals(intervalToCronSchedule({ value: 1, unit: "d" }), {
      dayOfMonth: { every: 1 },
    });
  });

  await t.step("converts exact multiples to coarser units", () => {
    assertEquals(intervalToCronSchedule({ value: 120, unit: "s" }), {
      minute: { every: 2 },
    });
    assertEquals(intervalToCronSchedule({ value: 120, unit: "m" }), {
      hour: { every: 2 },
    });
    assertEquals(intervalToCronSchedule({ value: 24, unit: "h" }), {
      dayOfMonth: { every: 1 },
    });
  });

  await t.step("throws for intervals Deno.cron cannot represent", () => {
    assertThrows(
      () => intervalToCronSchedule({ value: 90, unit: "s" }),
      Error,
      "Deno KV recurrence supports",
    );
    assertThrows(
      () => intervalToCronSchedule({ value: 90, unit: "m" }),
      Error,
      "Deno KV recurrence supports",
    );
    assertThrows(
      () => intervalToCronSchedule({ value: 25, unit: "h" }),
      Error,
      "Deno KV recurrence supports",
    );
    assertThrows(
      () => intervalToCronSchedule({ value: 32, unit: "d" }),
      Error,
      "Use the BullMQ backend for arbitrary elapsed intervals",
    );
    assertThrows(
      () => intervalToCronSchedule({ value: 7, unit: "m" }),
      Error,
      "true elapsed cadence",
    );
    assertThrows(
      () => intervalToCronSchedule({ value: 5, unit: "h" }),
      Error,
      "true elapsed cadence",
    );
    assertThrows(
      () => intervalToCronSchedule({ value: 48, unit: "h" }),
      Error,
      "true elapsed cadence",
    );
    assertThrows(
      () => intervalToCronSchedule({ value: 2, unit: "d" }),
      Error,
      "true elapsed cadence",
    );
  });
});

Deno.test("validateCronExpression", async (t) => {
  await t.step("validates 5-field cron expressions", () => {
    assertEquals(validateCronExpression("0 * * * *"), true);
    assertEquals(validateCronExpression("*/15 * * * *"), true);
    assertEquals(validateCronExpression("0 9 * * 1-5"), true);
    assertEquals(validateCronExpression("0 0 1 * *"), true);
  });

  await t.step("validates 6-field cron expressions (with seconds)", () => {
    assertEquals(validateCronExpression("0 0 * * * *"), true);
    assertEquals(validateCronExpression("*/30 * * * * *"), true);
  });

  await t.step("rejects invalid cron expressions", () => {
    assertEquals(validateCronExpression(""), false);
    assertEquals(validateCronExpression("abc"), false);
    assertEquals(validateCronExpression("* * *"), false);
    assertEquals(validateCronExpression("* * * * * * *"), false);
  });
});
