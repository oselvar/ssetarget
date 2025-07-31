import { describe } from "vitest";

import { MemoryEventStore } from "./MemoryEventStore";
import { runSSETargetTests } from "./SSETargetTests";

describe("MemoryEventStore", () => {
  runSSETargetTests(() => new MemoryEventStore());
});
