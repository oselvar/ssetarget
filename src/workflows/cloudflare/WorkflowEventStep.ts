import type {
  StepPromise,
  WorkflowSleepDuration,
  WorkflowStep,
  WorkflowStepConfig,
  WorkflowStepContext,
  WorkflowStepEvent,
  WorkflowTimeoutDuration,
} from "cloudflare:workers";

import type { DispatchEvent } from "./batchedDispatchEvent.js";

/**
 * Whether or not to send an event notification for a given step
 */
export type ShouldDispatch = (step: string) => boolean;

export class WorkflowEventStep implements WorkflowStep {
  constructor(
    private readonly step: WorkflowStep,
    private readonly instanceId: string,
    private readonly dispatchEvent: DispatchEvent,
    private readonly shouldDispatch: ShouldDispatch = () => true,
  ) {}

  do<T extends Rpc.Serializable<T>>(
    name: string,
    callback: (ctx: WorkflowStepContext) => Promise<T>,
  ): StepPromise<T>;
  do<T extends Rpc.Serializable<T>>(
    name: string,
    config: WorkflowStepConfig,
    callback: (ctx: WorkflowStepContext) => Promise<T>,
  ): StepPromise<T>;
  do<T extends Rpc.Serializable<T>>(
    name: string,
    configOrCallback: WorkflowStepConfig | ((ctx: WorkflowStepContext) => Promise<T>),
    maybeCallback?: (ctx: WorkflowStepContext) => Promise<T>,
  ): StepPromise<T> {
    const inner =
      typeof configOrCallback === "function"
        ? this.step.do<T>(name, configOrCallback)
        : this.step.do<T>(
            name,
            configOrCallback,
            maybeCallback as (ctx: WorkflowStepContext) => Promise<T>,
          );
    return withRollback(this.runWithEvents(name, inner), inner);
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
  ): StepPromise<WorkflowStepEvent<T>> {
    const inner = this.step.waitForEvent<T>(name, options);
    return withRollback(this.runWithEvents(name, inner), inner);
  }

  private async runWithEvents<R>(step: string, inner: Promise<R>): Promise<R> {
    if (!this.shouldDispatch(step)) {
      return inner;
    }
    const spanId = `${this.instanceId}#${step}-${crypto.randomUUID()}`;
    await this.dispatchEvent(this.instanceId, {
      type: "started",
      spanId,
      parentSpanId: this.instanceId,
      kind: "step",
      name: step,
      attributes: {},
      timestamp: new Date().toISOString(),
    });

    try {
      const result = await inner;
      await this.dispatchEvent(this.instanceId, {
        type: "ended",
        spanId,
        status: "completed",
        attributes: {},
        timestamp: new Date().toISOString(),
      });
      return result;
    } catch (error) {
      await this.dispatchEvent(this.instanceId, {
        type: "ended",
        spanId,
        status: "failed",
        attributes: {},
        timestamp: new Date().toISOString(),
        error: error instanceof Error ? error.message : String(error),
      });
      throw error;
    }
  }
}

function withRollback<R>(promise: Promise<R>, source: StepPromise<R>): StepPromise<R> {
  const result = promise as StepPromise<R>;
  result.rollback = source.rollback.bind(source);
  return result;
}
