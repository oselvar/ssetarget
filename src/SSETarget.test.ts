import { createEventSource } from "eventsource-client";
import { describe, expect, it, vi } from "vitest";

import { type EventStore } from "./EventStore.js";
import { MemoryEventStore } from "./MemoryEventStore.js";
import { NullEventStore } from "./NullEventStore.js";
import { type ServerSentEvent, SSETarget } from "./SSETarget.js";
import { runSSETargetTests } from "./SSETargetTests.js";

describe("SSETarget with MemoryEventStore", () => {
  runSSETargetTests(() => new MemoryEventStore());
});

describe("SSETarget with NullEventStore", () => {
  runSSETargetTests(() => new NullEventStore());
});

type TestEvent = ServerSentEvent & {
  thing: string;
};

function subscriberCount(sse: SSETarget<TestEvent>): number {
  return (sse as unknown as { subscribers: Set<unknown> }).subscribers.size;
}

describe("SSETarget", () => {
  it("cleans up the subscriber when the event store fails", async () => {
    // streamSSE close()s the stream when the callback throws, without firing
    // abort listeners — cleanup must not depend on onAbort alone, or a failing
    // store would leak the subscriber (and its ever-growing queue) forever.
    const failingStore: EventStore<TestEvent> = {
      storeEvent: async (event) => event,
      getEvents: async () => {
        throw new Error("store unavailable");
      },
    };
    const consoleError = vi.spyOn(console, "error").mockImplementation(() => {});
    try {
      const sse = new SSETarget("/sse", failingStore);
      const response = await sse.fetch(new Request("http://0.0.0.0/sse"));

      // Drain the stream to completion: it ends when streamSSE closes it
      // after the callback has thrown.
      const reader = (response.body as ReadableStream<Uint8Array>).getReader();
      while (!(await reader.read()).done) {
        // discard
      }

      expect(subscriberCount(sse)).toBe(0);
    } finally {
      consoleError.mockRestore();
    }
  });

  it("delivers an event that becomes visible in the store after a concurrent replay", async () => {
    // With concurrent dispatches, an event can be assigned a lower id but
    // become visible to getEvents *after* an event with a higher id. The
    // replay/queue deduplication must skip exactly the replayed events —
    // not everything below the highest replayed id.
    let releaseGetEvents!: () => void;
    const gate = new Promise<void>((resolve) => (releaseGetEvents = resolve));
    const visible: TestEvent[] = [];
    let nextId = 1;
    const store: EventStore<TestEvent> = {
      async storeEvent(event) {
        const eventWithId = { ...event, id: nextId++ };
        // Event 1's store write is still "in flight": it has an id, but is
        // not yet visible to getEvents.
        if (eventWithId.id !== 1) {
          visible.push(eventWithId);
        }
        return eventWithId;
      },
      async getEvents(lastEventId) {
        await gate;
        return visible.filter((event) => (event.id ?? 0) > lastEventId);
      },
    };

    const sse = new SSETarget("/sse", store);

    const received = await new Promise<readonly string[]>((resolve, reject) => {
      const things: string[] = [];
      const es = createEventSource({
        url: "http://0.0.0.0/sse",
        fetch: (url) => sse.fetch(new Request(url)),
        onConnect() {
          // The connection is now blocked in getEvents on the gate. Dispatch
          // both events (queued for the live loop), then let the replay see
          // only event 2.
          (async () => {
            await sse.dispatchEvent({ type: "test", thing: "one" });
            await sse.dispatchEvent({ type: "test", thing: "two" });
            releaseGetEvents();
          })().catch(reject);
        },
        onMessage({ event, data }) {
          if (event === "ping") return;
          things.push((JSON.parse(data) as { thing: string }).thing);
          if (things.length === 2) {
            es.close();
            resolve(things);
          }
        },
      });
    });

    // "two" arrives first via the replay; "one" must still arrive via the
    // queue instead of being dropped by the deduplication.
    expect(received).toEqual(["two", "one"]);
  });
});
