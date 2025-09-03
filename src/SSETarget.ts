import { Hono } from "hono";
import { streamSSE } from "hono/streaming";

import { type EventStore } from "./EventStore";

export type ServerSentEvent = {
  type: string;
  id?: number;
};

/**
 * Receives events and dispatches them to connected EventSource clients.
 */
export class SSETarget<E extends ServerSentEvent> {
  private readonly app: Hono;
  private eventResolvers: Array<(event: readonly E[]) => void> = [];
  private pendingEvents: E[] = [];

  constructor(
    private ssePath: string,
    private readonly eventStore: EventStore<E>,
    private pingIntervalMillis = 10_000,
  ) {
    this.app = new Hono().get(this.ssePath, (c) => {
      return streamSSE(c, async (stream) => {
        const ping = setInterval(() => {
          stream.writeSSE({ event: "ping", data: "" }).catch((err) => {
            // eslint-disable-next-line no-console
            console.error("SSE Error writing ping", err);
          });
        }, this.pingIntervalMillis);

        let loop = true;
        stream.onAbort(() => {
          loop = false;
          clearInterval(ping);
        });

        const lastEventIdHeader = c.req.header("Last-Event-ID");

        if (lastEventIdHeader) {
          const lastEventId = parseInt(lastEventIdHeader, 10);
          if (!isNaN(lastEventId)) {
            const storedEvents = await this.eventStore.getEvents(lastEventId);

            for (const event of storedEvents) {
              const { id, type, ...rest } = event;
              await stream.writeSSE({
                id: id === undefined ? undefined : String(id),
                event: type,
                data: JSON.stringify(rest),
              });
            }
          }
        }

        while (loop) {
          const events = await this.waitForNewEvents();

          for (const event of events) {
            const { id, type, ...rest } = event;
            await stream.writeSSE({
              id: id === undefined ? undefined : String(id),
              event: type,
              data: JSON.stringify(rest),
            });
          }
        }
      });
    });
  }

  /**
   * Dispatches an event to connected EventSource clients.
   * @param event the event object to dispatch.
   */
  async dispatchEvent(event: E) {
    const eventWithId = await this.eventStore.storeEvent(event);

    // Notify waiting streams about the new event
    this.notifyNewEvent(eventWithId);
  }

  async fetch(request: Request) {
    return this.app.fetch(request);
  }

  private notifyNewEvent(event: E) {
    // Resolve all waiting promises with the new event
    if (this.eventResolvers.length > 0) {
      const resolvers = this.eventResolvers;
      this.eventResolvers = [];
      resolvers.forEach((resolve) => resolve([event]));
    } else {
      // Only store pending events if there are no waiting resolvers
      this.pendingEvents.push(event);
    }
  }

  private waitForNewEvents(): Promise<readonly E[]> {
    if (this.pendingEvents.length > 0) {
      const eventsToReturn = [...this.pendingEvents];
      this.pendingEvents = [];
      return Promise.resolve(eventsToReturn);
    }
    return new Promise((resolve) => this.eventResolvers.push(resolve));
  }
}
