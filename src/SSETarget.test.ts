import { describe } from "vitest";

import { MemoryEventStore } from "./MemoryEventStore";
import { NullEventStore } from "./NullEventStore";
import { runSSETargetTests } from "./SSETargetTests";

describe("SSETarget with MemoryEventStore", () => {
  runSSETargetTests(() => new MemoryEventStore());
});

describe("SSETarget with NullEventStore", () => {
  runSSETargetTests(() => new NullEventStore());
});
