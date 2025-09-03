import { type EventStore } from "../../EventStore";
import type { StepEvent } from "..";

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

  async storeEvent(event: StepEvent): Promise<StepEvent> {
    const sql = this.ctx.storage.sql;
    const query = `INSERT INTO events (taskId, type, step, timestamp, error) VALUES (?, ?, ?, ?, ?) RETURNING id`;
    const result = sql.exec(
      query,
      ...[event.taskId, event.type, event.step, event.timestamp, event.error],
    );
    const row = result.one();
    if (!row) {
      throw new Error("Failed to insert event");
    }
    return { ...event, id: row["id"] as number } as StepEvent;
  }

  async getEvents(lastEventId: number): Promise<readonly StepEvent[]> {
    const sql = this.ctx.storage.sql;
    return sql
      .exec<StepEvent>("SELECT * FROM events WHERE id > ? ORDER BY id ASC", lastEventId)
      .toArray();
  }
}
