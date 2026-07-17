import type { IncomingMessage } from "node:http";

import { Hono } from "hono";
import { type SSEStreamingApi, streamSSE } from "hono/streaming";

import { type EventStore } from "./EventStore.js";

export type ServerSentEvent = {
  type: string;
  id?: number;
};

type Subscriber<E> = {
  queue: E[];
  notify: (() => void) | undefined;
};

/**
 * Receives events and dispatches them to connected EventSource clients.
 */
export class SSETarget<E extends ServerSentEvent> {
  private readonly app: Hono;
  private readonly subscribers = new Set<Subscriber<E>>();

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
        // Subscribe synchronously, before the first await: streamSSE starts
        // this callback eagerly, so the subscriber is registered before the
        // Response is returned to the client. Events dispatched from then on
        // land in this connection's queue; events dispatched during the
        // stored-event replay below would otherwise fall into a gap. Events
        // present in both the replay and the queue are deduplicated by id.
        const subscriber: Subscriber<E> = { queue: [], notify: undefined };
        this.subscribers.add(subscriber);

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
          this.subscribers.delete(subscriber);
          const notify = subscriber.notify;
          subscriber.notify = undefined;
          notify?.();
        });

        // Flush the response on connect, before any event is available. Some
        // servers (notably SvelteKit's adapter-node) only write the response
        // headers once the first body byte is produced, so without this an
        // EventSource stays stuck "connecting" until the first event or ping.
        // The trailing comment drains the first one through immediately (same
        // lazy-flush reason writeFlushed exists below).
        await stream.write(": connected\n\n");
        await stream.write(": flush\n\n");

        // The native browser EventSource can't set request headers, so it
        // can't send Last-Event-ID on the *initial* connection. To let such
        // clients resume from a known point, we also accept the id as a
        // `lastEventId` query parameter. The header still wins when present —
        // the browser sets it automatically on reconnects, where it reflects
        // the most recently received event.
        const lastEventIdValue = c.req.header("Last-Event-ID") || c.req.query("lastEventId") || "0";

        const lastEventId = parseInt(lastEventIdValue, 10);
        let lastReplayedId = Number.NEGATIVE_INFINITY;
        if (!isNaN(lastEventId)) {
          const storedEvents = await this.eventStore.getEvents(lastEventId);

          for (const event of storedEvents) {
            const { id, type, ...rest } = event;
            await writeFlushed(stream, {
              id: id === undefined ? undefined : String(id),
              event: type,
              data: JSON.stringify(rest),
            });
            if (id !== undefined) {
              lastReplayedId = id;
            }
          }
        }

        while (loop) {
          const events = await waitForNewEvents(subscriber);

          for (const event of events) {
            const { id, type, ...rest } = event;
            if (id !== undefined && id <= lastReplayedId) {
              continue;
            }
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
    // Append the event to every subscriber's own queue, and wake up the
    // subscribers that are currently waiting. Each connection drains only
    // its own queue, so a busy connection never steals events from another.
    for (const subscriber of this.subscribers) {
      subscriber.queue.push(event);
      const notify = subscriber.notify;
      if (notify) {
        subscriber.notify = undefined;
        notify();
      }
    }
  }
}

function waitForNewEvents<E>(subscriber: Subscriber<E>): Promise<readonly E[]> {
  if (subscriber.queue.length > 0) {
    return Promise.resolve(drainQueue(subscriber));
  }
  return new Promise((resolve) => {
    subscriber.notify = () => resolve(drainQueue(subscriber));
  });
}

function drainQueue<E>(subscriber: Subscriber<E>): readonly E[] {
  const events = subscriber.queue;
  subscriber.queue = [];
  return events;
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
