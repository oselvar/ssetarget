import { Hono } from "hono";
import { streamSSE } from "hono/streaming";

export type ServerSentEvent = {
  type: string;
};

export type ServerSentEventWithId<E extends ServerSentEvent> = E & {
  id: number;
};

/**
 * Receives events and dispatches them to connected EventSource clients.
 *
 * New clients will receive previously dispatched events.
 *
 * *IMPORTANT:* This class is meant to be subclassed, overriding storeEvent and getEvents.
 * The default implementation stores events in memory, which may cause out of memory errors.
 */
export abstract class SSETarget<E extends ServerSentEvent> {
  private eventResolvers: Array<() => void> = [];

  constructor(
    private readonly ssePath: string,
    private readonly pingIntervalMillis = 10_000,
  ) {}

  /**
   * Dispatches an event to connected EventSource clients.
   * @param event the event object to dispatch.
   */
  dispatchEvent(event: E) {
    this.storeEvent(event);

    // Notify waiting streams about the new event
    this.notifyNewEvent();
  }

  protected abstract storeEvent(event: E): void;

  protected abstract getEvents(lastEventId: number): readonly ServerSentEventWithId<E>[];

  async fetch(request: Request) {
    const app = new Hono<{ Bindings: Env }>();
    app.get(this.ssePath, (c) => {
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
        let lastEventId = 0;

        if (lastEventIdHeader) {
          lastEventId = parseInt(lastEventIdHeader, 10);
          if (isNaN(lastEventId)) {
            // Start from the event after the last received event
            lastEventId = 0;
          }
        }

        while (loop) {
          const newEvents = this.getEvents(lastEventId);

          for (const event of newEvents) {
            const { id, type, ...rest } = event;
            await stream.writeSSE({
              id: String(id),
              event: type,
              data: JSON.stringify(rest),
            });
            lastEventId = id;
          }

          await this.waitForNewEvent();
        }
      });
    });

    return app.fetch(request);
  }

  private notifyNewEvent() {
    // Resolve all waiting promises
    const resolvers = this.eventResolvers;
    this.eventResolvers = [];
    resolvers.forEach((resolve) => resolve());
  }

  private waitForNewEvent(): Promise<void> {
    return new Promise((resolve) => this.eventResolvers.push(resolve));
  }
}
