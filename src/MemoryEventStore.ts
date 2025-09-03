import { type EventStore } from "./EventStore";
import { type ServerSentEvent, type ServerSentEventWithId } from "./SSETarget";

export class MemoryEventStore<E extends ServerSentEvent> implements EventStore<E> {
  private events: ServerSentEventWithId<E>[] = [];

  async storeEvent(event: E): Promise<void> {
    this.events.push({ ...event, id: this.events.length + 1 } as ServerSentEventWithId<E>);
  }

  async getEvents(lastEventId: number): Promise<readonly ServerSentEventWithId<E>[]> {
    return this.events.filter((event) => event.id > lastEventId);
  }
}
