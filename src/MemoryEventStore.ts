import { type EventStore } from "./EventStore.js";
import { type ServerSentEvent } from "./SSETarget.js";

export class MemoryEventStore<E extends ServerSentEvent> implements EventStore<E> {
  private events: E[] = [];

  async storeEvent(event: E): Promise<E> {
    const eventWithId = { ...event, id: this.events.length + 1 };
    this.events.push(eventWithId);
    return eventWithId;
  }

  async getEvents(lastEventId: number): Promise<readonly E[]> {
    return this.events.filter((event) => event.id === undefined || event.id > lastEventId);
  }
}
