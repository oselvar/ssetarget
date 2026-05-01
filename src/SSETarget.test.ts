import { describe } from "vitest";

import { MemoryEventStore } from "./MemoryEventStore.js";
import { NullEventStore } from "./NullEventStore.js";
import { runSSETargetTests } from "./SSETargetTests.js";

describe("SSETarget with MemoryEventStore", () => {
  runSSETargetTests(() => new MemoryEventStore());
});

describe("SSETarget with NullEventStore", () => {
  runSSETargetTests(() => new NullEventStore());
});
