import { type ServerSentEvent, type ServerSentEventWithId } from "./SSETarget";

export interface EventStore<E extends ServerSentEvent> {
  storeEvent(event: E): Promise<void>;
  getEvents(lastEventId: number): Promise<readonly ServerSentEventWithId<E>[]>;
}
