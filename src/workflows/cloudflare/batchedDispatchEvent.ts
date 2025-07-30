import type { StepEvent } from "..";
import type { WorkflowEvents } from "./WorkflowEvents";

export type DispatchEvent = (instanceId: string, event: StepEvent) => Promise<void>;

export function synchronousDispatchEvent<T extends WorkflowEvents<object>>(
  ctx: ExecutionContext,
  workflowEventsNs: DurableObjectNamespace<T>,
): DispatchEvent {
  return async (runId, event) => {
    const eventsDurableObject = workflowEventsNs.get(workflowEventsNs.idFromName(runId));
    ctx.waitUntil(eventsDurableObject.dispatchEvent(event));
  };
}

/**
 * Creates a function that batches events and dispatches them after a delay.
 * This is useful to avoid spamming the DO with dispatchEvent calls, which may
 * lead to rate limiting (Too many API requests by single worker invocation.)
 *
 * @param ctx - The execution context.
 * @param workflowEventsNs - The namespace of the workflow events.
 * @param delay - The delay in milliseconds before dispatching the events.
 * @returns A function that dispatches the events.
 */
export function batchedDispatchEvent<T extends WorkflowEvents<object>>(
  ctx: ExecutionContext,
  workflowEventsNs: DurableObjectNamespace<T>,
  delay: number,
): DispatchEvent {
  const batchedEvents: Record<string, StepEvent[]> = {};
  const pendingTimers: Record<string, NodeJS.Timeout> = {};

  return async (runId, event) => {
    // Atomically add event to batch
    if (!batchedEvents[runId]) {
      batchedEvents[runId] = [];
    }
    batchedEvents[runId].push(event);

    // Only set up timer if one doesn't already exist
    if (!pendingTimers[runId]) {
      const timerPromise = new Promise<void>((resolve, reject) => {
        pendingTimers[runId] = setTimeout(async () => {
          try {
            // Atomically extract and clear the batch
            const events = batchedEvents[runId] || [];
            delete batchedEvents[runId];
            delete pendingTimers[runId];

            if (events.length > 0) {
              const eventsDurableObject = workflowEventsNs.get(workflowEventsNs.idFromName(runId));
              await eventsDurableObject.dispatchEvent(events);
            }
            resolve();
          } catch (error) {
            reject(error);
          }
        }, delay);
      });

      ctx.waitUntil(timerPromise);
    }
  };
}
