import type {
  WorkflowSleepDuration,
  WorkflowStep,
  WorkflowStepConfig,
  WorkflowStepEvent,
  WorkflowTimeoutDuration,
} from "cloudflare:workers";

import type { DispatchEvent } from "./batchedDispatchEvent";

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

  async do<T extends Rpc.Serializable<T>>(
    name: string,
    configOrTask: WorkflowStepConfig | (() => Promise<T>),
    callback?: () => Promise<T>,
  ): Promise<T> {
    return this.withEvents(name, () =>
      this.step.do(name, configOrTask as WorkflowStepConfig, callback as () => Promise<T>),
    );
  }

  async sleep(name: string, duration: WorkflowSleepDuration): Promise<void> {
    return this.withEvents(name, () => this.step.sleep(name, duration));
  }

  async sleepUntil(name: string, timestamp: Date | number): Promise<void> {
    return this.withEvents(name, () => this.step.sleepUntil(name, timestamp));
  }

  async waitForEvent<T extends Rpc.Serializable<T>>(
    name: string,
    options: {
      type: string;
      timeout?: WorkflowTimeoutDuration | number;
    },
  ): Promise<WorkflowStepEvent<T>> {
    return this.withEvents(name, () => this.step.waitForEvent(name, options));
  }

  private async withEvents<R>(step: string, callback: () => Promise<R>): Promise<R> {
    if (!this.shouldDispatch(step)) {
      return callback();
    }
    const taskId = crypto.randomUUID();
    await this.dispatchEvent(this.instanceId, {
      type: "started",
      taskId,
      step,
      timestamp: new Date().toISOString(),
    });

    try {
      const result = await callback();
      await this.dispatchEvent(this.instanceId, {
        type: "completed",
        taskId,
        step,
        timestamp: new Date().toISOString(),
      });
      return result;
    } catch (error) {
      await this.dispatchEvent(this.instanceId, {
        type: "failed",
        taskId,
        step,
        timestamp: new Date().toISOString(),
        error: error instanceof Error ? error.message : String(error),
      });
      throw error;
    }
  }
}
