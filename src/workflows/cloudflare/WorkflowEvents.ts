import { DurableObject } from "cloudflare:workers";

import { SSETarget } from "../../SSETarget.js";
import type { StepEvent } from "../index.js";
import { StepEventStore } from "./StepEventStore.js";

export class WorkflowEvents<Env extends object> extends DurableObject<Env> {
  private sseTarget: SSETarget<StepEvent>;

  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env);
    const eventStore = new StepEventStore(ctx);
    this.sseTarget = new SSETarget("*", eventStore);
  }

  async dispatchEvent(event: StepEvent | readonly StepEvent[]) {
    const eventsArray = Array.isArray(event) ? event : [event];
    for (const event of eventsArray) {
      await this.sseTarget.dispatchEvent(event);
    }
  }

  override async fetch(request: Request) {
    return this.sseTarget.fetch(request);
  }
}
