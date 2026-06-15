import type {
  WorkflowSleepDuration,
  WorkflowStep,
  WorkflowStepConfig,
  WorkflowStepContext,
  WorkflowStepEvent,
  WorkflowStepRollbackOptions,
  WorkflowTimeoutDuration,
} from "cloudflare:workers";

import { Tracer } from "../Tracer.js";
import type { DispatchEvent } from "./batchedDispatchEvent.js";

/**
 * Whether or not to send an event notification for a given step
 */
export type ShouldDispatch = (step: string) => boolean;

export class WorkflowEventStep implements WorkflowStep {
  private readonly tracer: Tracer;

  constructor(
    private readonly step: WorkflowStep,
    private readonly instanceId: string,
    dispatchEvent: DispatchEvent,
    private readonly shouldDispatch: ShouldDispatch = () => true,
  ) {
    this.tracer = new Tracer((event) => dispatchEvent(instanceId, event), instanceId);
  }

  /**
   * Wrap the workflow body so a root span is emitted for the whole instance.
   * The root span's spanId is the workflow instanceId; child step spans
   * reference it as their parentSpanId.
   */
  async withWorkflow<T>(name: string, fn: () => Promise<T>): Promise<T> {
    return this.tracer.withSpan({ spanId: this.instanceId, parentSpanId: null, name }, () => fn());
  }

  do<T extends Rpc.Serializable<T>>(
    name: string,
    callback: (ctx: WorkflowStepContext) => Promise<T>,
    rollbackOptions?: WorkflowStepRollbackOptions<T>,
  ): Promise<T>;
  do<T extends Rpc.Serializable<T>>(
    name: string,
    config: WorkflowStepConfig,
    callback: (ctx: WorkflowStepContext) => Promise<T>,
    rollbackOptions?: WorkflowStepRollbackOptions<T>,
  ): Promise<T>;
  do<T extends Rpc.Serializable<T>>(
    name: string,
    configOrCallback: WorkflowStepConfig | ((ctx: WorkflowStepContext) => Promise<T>),
    callbackOrRollback?:
      | ((ctx: WorkflowStepContext) => Promise<T>)
      | WorkflowStepRollbackOptions<T>,
    maybeRollbackOptions?: WorkflowStepRollbackOptions<T>,
  ): Promise<T> {
    const inner =
      typeof configOrCallback === "function"
        ? this.step.do<T>(
            name,
            configOrCallback,
            callbackOrRollback as WorkflowStepRollbackOptions<T> | undefined,
          )
        : this.step.do<T>(
            name,
            configOrCallback,
            callbackOrRollback as (ctx: WorkflowStepContext) => Promise<T>,
            maybeRollbackOptions,
          );
    return this.runWithEvents(name, inner);
  }

  sleep(name: string, duration: WorkflowSleepDuration): Promise<void> {
    return this.runWithEvents(name, this.step.sleep(name, duration));
  }

  sleepUntil(name: string, timestamp: Date | number): Promise<void> {
    return this.runWithEvents(name, this.step.sleepUntil(name, timestamp));
  }

  waitForEvent<T extends Rpc.Serializable<T>>(
    name: string,
    options: {
      type: string;
      timeout?: WorkflowTimeoutDuration | number;
    },
  ): Promise<WorkflowStepEvent<T>> {
    return this.runWithEvents(name, this.step.waitForEvent<T>(name, options));
  }

  private async runWithEvents<R>(name: string, inner: Promise<R>): Promise<R> {
    if (!this.shouldDispatch(name)) {
      return inner;
    }
    return this.tracer.withSpan(
      {
        spanId: crypto.randomUUID(),
        parentSpanId: this.instanceId,
        name,
      },
      () => inner,
    );
  }
}
