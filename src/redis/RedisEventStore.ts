import type Redis from "ioredis";

import { type EventStore } from "../EventStore";
import { type ServerSentEvent } from "../SSETarget";

export class RedisEventStore<E extends ServerSentEvent> implements EventStore<E> {
  private readonly eventsKey: string;
  private readonly counterKey: string;

  constructor(
    private readonly redis: Redis,
    keyPrefix = "ssetarget",
  ) {
    this.eventsKey = `${keyPrefix}:events`;
    this.counterKey = `${keyPrefix}:counter`;
  }

  async storeEvent(event: E): Promise<E> {
    const results = await this.redis.pipeline().incr(this.counterKey).exec();
    if (results) {
      const [[, eventId]] = results as [[null, number]];
      await this.redis.zadd(this.eventsKey, eventId, JSON.stringify(event));
      return { ...event, id: eventId };
    }
    throw new Error("Failed to store event in Redis");
  }

  async getEvents(lastEventId: number): Promise<readonly E[]> {
    const eventStrings = await this.redis.zrangebyscore(
      this.eventsKey,
      lastEventId + 1,
      "+inf",
      "WITHSCORES",
    );

    const events: E[] = [];
    for (let i = 0; i < eventStrings.length; i += 2) {
      const eventJson = eventStrings[i];
      const id = parseInt(eventStrings[i + 1] as string, 10);

      if (eventJson) {
        const event = JSON.parse(eventJson) as E;
        events.push({ ...event, id });
      }
    }

    return events;
  }
}
