import { type EventStore } from "./EventStore";
import { type ServerSentEvent } from "./SSETarget";

export class NullEventStore<E extends ServerSentEvent> implements EventStore<E> {
  async storeEvent(event: E): Promise<E> {
    // We're not adding an ID to the event
    return event;
  }

  async getEvents(_lastEventId: number): Promise<readonly E[]> {
    return [];
  }
}
