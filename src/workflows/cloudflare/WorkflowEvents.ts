import { DurableObject } from "cloudflare:workers";

import { SSETarget } from "../../SSETarget";
import type { StepEvent, StepEventWithId } from "..";

export class WorkflowEvents<Env extends object> extends DurableObject<Env> {
  private workflowSSE: DurableObjectSSETarget;

  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env);
    this.workflowSSE = new DurableObjectSSETarget(ctx);
  }

  async dispatchEvent(event: StepEvent | readonly StepEvent[]) {
    const eventsArray = Array.isArray(event) ? event : [event];
    for (const event of eventsArray) {
      this.workflowSSE.dispatchEvent(event);
    }
  }

  override async fetch(request: Request) {
    return this.workflowSSE.fetch(request);
  }
}

class DurableObjectSSETarget extends SSETarget<StepEvent> {
  constructor(private readonly ctx: DurableObjectState) {
    super("*");
    const sql = ctx.storage.sql;

    sql.exec(`CREATE TABLE IF NOT EXISTS events(
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      taskId TEXT NOT NULL,
      type TEXT NOT NULL,
      step TEXT NOT NULL,
      timestamp TEXT NOT NULL,
      error TEXT
    );`);
  }

  override storeEvent(event: StepEvent) {
    const sql = this.ctx.storage.sql;
    const query = `INSERT INTO events (taskId, type, step, timestamp, error) VALUES (?, ?, ?, ?, ?)`;
    sql.exec(query, ...[event.taskId, event.type, event.step, event.timestamp, event.error]);
  }

  override async getEvents(lastEventId: number): Promise<readonly StepEventWithId[]> {
    const sql = this.ctx.storage.sql;
    return sql
      .exec<StepEventWithId>("SELECT * FROM events WHERE id > ? ORDER BY id ASC", lastEventId)
      .toArray();
  }
}
