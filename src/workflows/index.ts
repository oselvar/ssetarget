import type { ServerSentEventWithId } from "../SSETarget";

export type StepEvent = {
  type: "started" | "completed" | "failed";
  step: string;
  taskId: string;
  timestamp: string;
  error?: string;
};

export type StepEventWithId = ServerSentEventWithId<StepEvent>;
