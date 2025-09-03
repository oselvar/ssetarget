import { type ServerSentEvent } from "./SSETarget";

export interface EventStore<E extends ServerSentEvent> {
  storeEvent(event: E): Promise<E>;
  getEvents(lastEventId: number): Promise<readonly E[]>;
}
