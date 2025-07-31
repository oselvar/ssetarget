import { MemorySSETarget } from "./MemorySSETarget";
import { runSSETargetTests } from "./SSETargetTests";

runSSETargetTests("MemorySSETarget", () => new MemorySSETarget("/sse"));
