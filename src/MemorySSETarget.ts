import { type ServerSentEvent, type ServerSentEventWithId, SSETarget } from "./SSETarget";

export class MemorySSETarget<E extends ServerSentEvent> extends SSETarget<E> {
  private events: ServerSentEventWithId<E>[] = [];

  protected override storeEvent(event: E): void {
    this.events.push({ ...event, id: this.events.length + 1 });
  }

  protected override async getEvents(
    lastEventId: number,
  ): Promise<readonly ServerSentEventWithId<ServerSentEventWithId<E>>[]> {
    return Promise.resolve(this.events.filter((event) => event.id > lastEventId));
  }
}
