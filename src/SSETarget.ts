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
  // Set when the queue was dropped because the connection fell too far
  // behind. The connection loop recovers the dropped events from the store.
  overflowed: boolean;
};

/**
 * Receives events and dispatches them to connected EventSource clients.
 */
export class SSETarget<E extends ServerSentEvent> {
  private readonly app: Hono;
  private readonly subscribers = new Set<Subscriber<E>>();

  /**
   * @param ssePath the Hono route pattern to serve SSE requests on.
   * @param eventStore persists events for replay (see the EventStore contract).
   * @param pingIntervalMillis how often to send keep-alive pings.
   * @param maxQueueLength per-connection cap on events buffered for a slow
   *   client. When exceeded, the buffer is dropped and the connection re-reads
   *   the missed events from the event store. Events the store cannot replay
   *   (e.g. with NullEventStore) are lost for that connection.
   */
  constructor(
    private ssePath: string,
    private readonly eventStore: EventStore<E>,
    private pingIntervalMillis = 10_000,
    private readonly maxQueueLength = 1024,
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
        const subscriber: Subscriber<E> = { queue: [], notify: undefined, overflowed: false };
        this.subscribers.add(subscriber);

        const ping = setInterval(() => {
          writeFlushed(stream, { event: "ping", data: "" }).catch((err) => {
            // eslint-disable-next-line no-console
            console.error("SSE Error writing ping", err);
          });
        }, this.pingIntervalMillis);

        let loop = true;
        const cleanup = () => {
          loop = false;
          clearInterval(ping);
          this.subscribers.delete(subscriber);
          const notify = subscriber.notify;
          subscriber.notify = undefined;
          notify?.();
        };
        // Cleanup runs from onAbort when the client disconnects, and from the
        // finally below when the callback throws (e.g. the event store fails):
        // streamSSE close()s the stream on error WITHOUT firing abort
        // listeners, so relying on onAbort alone would leak the subscriber
        // and the ping interval. cleanup is idempotent, so both may run.
        stream.onAbort(cleanup);

        try {
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
          const lastEventIdValue =
            c.req.header("Last-Event-ID") || c.req.query("lastEventId") || "0";

          const lastEventId = parseInt(lastEventIdValue, 10);

          // Ids of events written from the store, so the live loop can skip
          // events that landed in the queue while a replay was reading them
          // from the store. Membership (not id ordering) is what dedupes:
          // with concurrent dispatches an event can become visible in the
          // store after a replay that already returned a higher id, and an
          // ordering check would wrongly drop it.
          const replayedIds = new Set<number>();
          let lastDeliveredId = 0;

          const replay = async (fromId: number) => {
            const storedEvents = await this.eventStore.getEvents(fromId);
            for (const event of storedEvents) {
              const { id, type, ...rest } = event;
              await writeFlushed(stream, {
                id: id === undefined ? undefined : String(id),
                event: type,
                data: JSON.stringify(rest),
              });
              if (id !== undefined) {
                replayedIds.add(id);
                lastDeliveredId = Math.max(lastDeliveredId, id);
              }
            }
          };

          if (!isNaN(lastEventId)) {
            lastDeliveredId = lastEventId;
            await replay(lastEventId);
          }

          while (loop) {
            const events = await waitForNewEvents(subscriber);

            // The queue was dropped because this connection fell too far
            // behind: recover the missed events from the store. Events
            // dispatched while this replay runs land in the queue and are
            // deduplicated below, exactly like on connect.
            if (subscriber.overflowed) {
              subscriber.overflowed = false;
              await replay(lastDeliveredId);
            }

            for (const event of events) {
              const { id, type, ...rest } = event;
              // delete() rather than has(): each event is queued at most
              // once, so this also keeps the set from retaining replayed ids
              // for the lifetime of the connection.
              if (id !== undefined && replayedIds.delete(id)) {
                continue;
              }
              await writeFlushed(stream, {
                id: id === undefined ? undefined : String(id),
                event: type,
                data: JSON.stringify(rest),
              });
              if (id !== undefined) {
                lastDeliveredId = Math.max(lastDeliveredId, id);
              }
            }
          }
        } finally {
          cleanup();
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
      if (subscriber.queue.length > this.maxQueueLength) {
        // Drop the queue rather than grow it unboundedly for a slow
        // consumer. The connection loop re-reads the dropped events from
        // the store; events the store cannot replay are lost.
        subscriber.queue = [];
        subscriber.overflowed = true;
      }
      const notify = subscriber.notify;
      if (notify) {
        subscriber.notify = undefined;
        notify();
      }
    }
  }
}

function waitForNewEvents<E>(subscriber: Subscriber<E>): Promise<readonly E[]> {
  // An overflowed subscriber must wake even with an empty queue (it was
  // dropped), so the connection loop can resync from the store.
  if (subscriber.queue.length > 0 || subscriber.overflowed) {
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
