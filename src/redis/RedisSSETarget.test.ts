import { Redis } from "ioredis";
import { afterEach, beforeEach, describe, expect, it } from "vitest";

import { runSSETargetTests } from "../SSETargetTests";
import { RedisEventStore } from "./RedisEventStore";

describe("RedisEventStore", () => {
  let redis: Redis;

  beforeEach(async () => {
    redis = new Redis(8379);
    await redis.flushall();
  });

  afterEach(async () => {
    await redis.quit();
  });

  it("should assign incrementing IDs", async () => {
    const eventStore = new RedisEventStore(redis);
    const event = { type: "test", data: "test" };
    const eventWithId = await eventStore.storeEvent(event);
    expect(eventWithId.id).toBe(1);
    const eventWithId2 = await eventStore.storeEvent(event);
    expect(eventWithId2.id).toBe(2);
  });

  it("should get events after lastEventId", async () => {
    const eventStore = new RedisEventStore(redis);
    const event = { type: "test", data: "test" };
    await eventStore.storeEvent(event);
    await eventStore.storeEvent(event);
    const events = await eventStore.getEvents(1);
    expect(events).toHaveLength(1);
    expect(events[0]?.id).toBe(2);
  });

  runSSETargetTests(() => new RedisEventStore(redis));
});
