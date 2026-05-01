import type { WorkflowEvents } from "../WorkflowEvents.js";

export function serveSSE<T extends WorkflowEvents<object>>(
  instanceId: string,
  request: Request,
  workflowEventsNs: DurableObjectNamespace<T>,
) {
  const workflowEvents = workflowEventsNs.get(workflowEventsNs.idFromName(instanceId));
  return workflowEvents.fetch("http://0.0.0.0/sse", request);
}
