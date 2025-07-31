import { describe } from "vitest";

import { MemorySSETarget } from "./MemorySSETarget";
import { runSSETargetTests } from "./SSETargetTests";

describe("MemorySSETarget", () => {
  runSSETargetTests(() => new MemorySSETarget("/sse"));
});
