import type Redis from "ioredis";

import { type ServerSentEvent, type ServerSentEventWithId, SSETarget } from "../SSETarget";

export class RedisSSETarget<E extends ServerSentEvent> extends SSETarget<E> {
  private readonly eventsKey: string;
  private readonly counterKey: string;

  constructor(
    ssePath: string,
    private readonly redis: Redis,
    keyPrefix = "ssetarget",
    pingIntervalMillis = 10_000,
  ) {
    super(ssePath, pingIntervalMillis);
    this.eventsKey = `${keyPrefix}:events`;
    this.counterKey = `${keyPrefix}:counter`;
  }

  protected override async storeEvent(event: E): Promise<void> {
    const results = await this.redis.pipeline().incr(this.counterKey).exec();
    if (results) {
      const [[, eventId]] = results as [[null, number]];
      await this.redis.zadd(this.eventsKey, eventId, JSON.stringify(event));
    }
  }

  protected override async getEvents(
    lastEventId: number,
  ): Promise<readonly ServerSentEventWithId<E>[]> {
    const eventStrings = await this.redis.zrangebyscore(
      this.eventsKey,
      lastEventId + 1,
      "+inf",
      "WITHSCORES",
    );

    const events: ServerSentEventWithId<E>[] = [];
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
