import type { IncomingMessage } from "node:http";

import { Hono } from "hono";
import { type SSEStreamingApi, streamSSE } from "hono/streaming";

import { type EventStore } from "./EventStore.js";

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
      // Tell nginx (and several CDN proxies) not to buffer the SSE response.
      c.header("X-Accel-Buffering", "no");

      // When running on @hono/node-server, disable Nagle's algorithm so each
      // chunk written to the response is sent on the wire immediately instead
      // of being coalesced by the kernel TCP stack. No-op on runtimes that
      // don't expose a Node socket (e.g. Cloudflare Workers).
      const incoming = (c.env as { incoming?: IncomingMessage } | undefined)?.incoming;
      incoming?.socket?.setNoDelay(true);

      return streamSSE(c, async (stream) => {
        const ping = setInterval(() => {
          writeFlushed(stream, { event: "ping", data: "" }).catch((err) => {
            // eslint-disable-next-line no-console
            console.error("SSE Error writing ping", err);
          });
        }, this.pingIntervalMillis);

        let loop = true;
        stream.onAbort(() => {
          loop = false;
          clearInterval(ping);
        });

        // The native browser EventSource can't set request headers, so it
        // can't send Last-Event-ID on the *initial* connection. To let such
        // clients resume from a known point, we also accept the id as a
        // `lastEventId` query parameter. The header still wins when present —
        // the browser sets it automatically on reconnects, where it reflects
        // the most recently received event.
        const lastEventIdValue = c.req.header("Last-Event-ID") || c.req.query("lastEventId") || "0";

        const lastEventId = parseInt(lastEventIdValue, 10);
        if (!isNaN(lastEventId)) {
          const storedEvents = await this.eventStore.getEvents(lastEventId);

          for (const event of storedEvents) {
            const { id, type, ...rest } = event;
            await writeFlushed(stream, {
              id: id === undefined ? undefined : String(id),
              event: type,
              data: JSON.stringify(rest),
            });
          }
        }

        while (loop) {
          const events = await this.waitForNewEvents();

          for (const event of events) {
            const { id, type, ...rest } = event;
            await writeFlushed(stream, {
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

// Hono's streamSSE writes through a TransformStream whose readable side pulls
// chunks lazily. Without a follow-up write, the last chunk sits queued until
// the next writeSSE (e.g. the next ping) drains it. Writing a SSE comment line
// (which clients ignore per the spec) right after the event forces the event
// chunk through immediately.
async function writeFlushed(
  stream: SSEStreamingApi,
  message: Parameters<SSEStreamingApi["writeSSE"]>[0],
): Promise<void> {
  await stream.writeSSE(message);
  await stream.write(": flush\n\n");
}
