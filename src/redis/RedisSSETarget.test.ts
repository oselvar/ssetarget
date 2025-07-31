import { Redis } from "ioredis";
import { afterEach, beforeEach, describe } from "vitest";

import { runSSETargetTests } from "../SSETargetTests";
import { RedisSSETarget } from "./RedisSSETarget";

describe("RedisSSETarget", () => {
  let redis: Redis;

  beforeEach(async () => {
    redis = new Redis(8379);
    await redis.flushall();
  });

  afterEach(async () => {
    await redis.quit();
  });

  runSSETargetTests(() => new RedisSSETarget("/sse", redis));
});
