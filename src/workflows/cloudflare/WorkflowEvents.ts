import { DurableObject } from "cloudflare:workers";

import { SSETarget } from "../../SSETarget.js";
import type { SpanEvent } from "../index.js";
import { SpanEventStore } from "./SpanEventStore.js";

export class WorkflowEvents<Env extends object> extends DurableObject<Env> {
  private sseTarget: SSETarget<SpanEvent>;

  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env);
    const eventStore = new SpanEventStore(ctx);
    this.sseTarget = new SSETarget("*", eventStore);
  }

  async dispatchEvent(event: SpanEvent | readonly SpanEvent[]) {
    const eventsArray = Array.isArray(event) ? event : [event];
    for (const event of eventsArray) {
      await this.sseTarget.dispatchEvent(event);
    }
  }

  override async fetch(request: Request) {
    return this.sseTarget.fetch(request);
  }
}
