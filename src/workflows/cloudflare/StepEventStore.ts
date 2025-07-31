import { type EventStore } from "../../EventStore";
import { type ServerSentEventWithId } from "../../SSETarget";
import type { StepEvent, StepEventWithId } from "..";

export class StepEventStore implements EventStore<StepEvent> {
  constructor(private readonly ctx: DurableObjectState) {
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

  async storeEvent(event: StepEvent): Promise<void> {
    const sql = this.ctx.storage.sql;
    const query = `INSERT INTO events (taskId, type, step, timestamp, error) VALUES (?, ?, ?, ?, ?)`;
    sql.exec(query, ...[event.taskId, event.type, event.step, event.timestamp, event.error]);
  }

  async getEvents(lastEventId: number): Promise<readonly ServerSentEventWithId<StepEvent>[]> {
    const sql = this.ctx.storage.sql;
    return sql
      .exec<StepEventWithId>("SELECT * FROM events WHERE id > ? ORDER BY id ASC", lastEventId)
      .toArray();
  }
}
