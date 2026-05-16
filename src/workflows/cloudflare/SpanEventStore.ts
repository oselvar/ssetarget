import { type EventStore } from "../../EventStore.js";
import type { SpanEvent } from "../index.js";

export class SpanEventStore implements EventStore<SpanEvent> {
  constructor(private readonly ctx: DurableObjectState) {
    const sql = ctx.storage.sql;

    sql.exec(`CREATE TABLE IF NOT EXISTS events(
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      payload TEXT NOT NULL
    );`);
  }

  async storeEvent(event: SpanEvent): Promise<SpanEvent> {
    const sql = this.ctx.storage.sql;
    const result = sql.exec(
      "INSERT INTO events (payload) VALUES (?) RETURNING id",
      JSON.stringify(event),
    );
    const row = result.one();
    if (!row) {
      throw new Error("Failed to insert event");
    }
    return { ...event, id: row["id"] as number } as SpanEvent;
  }

  async getEvents(lastEventId: number): Promise<readonly SpanEvent[]> {
    const sql = this.ctx.storage.sql;
    const rows = sql
      .exec<{ id: number; payload: string }>(
        "SELECT id, payload FROM events WHERE id > ? ORDER BY id ASC",
        lastEventId,
      )
      .toArray();
    return rows.map((row) => ({ ...(JSON.parse(row.payload) as SpanEvent), id: row.id }));
  }
}
