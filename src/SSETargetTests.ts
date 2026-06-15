import { createEventSource } from "eventsource-client";
import { expect, it } from "vitest";

import { type EventStore } from "./EventStore.js";
import { NullEventStore } from "./NullEventStore.js";
import { type ServerSentEvent, SSETarget } from "./SSETarget.js";

type TestEvent = ServerSentEvent & {
  thing: string;
};

const events: TestEvent[] = [
  {
    type: "test",
    thing: "apple",
  },
  {
    type: "test",
    thing: "banana",
  },
  {
    type: "test",
    thing: "cherry",
  },
];

const lastEvent = events[events.length - 1] as TestEvent;

function isEqual(event1: TestEvent, event2: TestEvent): boolean {
  return event1.type === event2.type && event1.thing == event2.thing;
}

export function runSSETargetTests(createEventStore: () => EventStore<TestEvent>) {
  it("flushes an initial comment on connect, before any event is dispatched", async () => {
    // Servers that only write response headers once the first body byte is
    // produced (e.g. SvelteKit's adapter-node) leave an EventSource stuck
    // "connecting" until the first event or ping arrives. SSETarget must emit
    // an initial byte on connect so the headers flush immediately. This reads
    // the first chunk of the stream WITHOUT dispatching any event: without the
    // initial flush this read would hang until the ping interval.
    const sse = new SSETarget("/sse", createEventStore());
    const response = await sse.fetch(new Request("http://0.0.0.0/sse"));
    const reader = (response.body as ReadableStream<Uint8Array>).getReader();
    const { value } = await reader.read();
    await reader.cancel();
    const text = new TextDecoder().decode(value);
    expect(text.startsWith(":")).toBe(true);
  });

  it("should dispatch events to EventSource", async () => {
    const eventStore = createEventStore();
    const sse = new SSETarget("/sse", eventStore);

    async function dispatchEvents() {
      for (const event of events) {
        await sse.dispatchEvent(event);
      }
    }

    const receivedEvents: TestEvent[] = [];

    await new Promise<void>((resolve, reject) => {
      const es = createEventSource({
        url: "http://0.0.0.0/sse",
        fetch: (url) => {
          const req = new Request(url);
          return sse.fetch(req);
        },
        onConnect() {
          // Dispatch events *after* the EventSource connects
          dispatchEvents().catch(reject);
        },
        onMessage({ event, data }) {
          const reconstructedEvent = { ...JSON.parse(data), type: event };
          receivedEvents.push(reconstructedEvent);
          if (isEqual(reconstructedEvent, lastEvent)) {
            es.close();
            resolve();
          }
        },
      });
    });

    expect(receivedEvents).toEqual(events);
  });

  it("should dispatch old events to EventSource with lastEventId", async () => {
    const eventStore = createEventStore();

    if (eventStore instanceof NullEventStore) {
      return;
    }

    const sse = new SSETarget("/sse", eventStore);

    // Dispatch events *before* the EventSource connects
    for (const event of events) {
      await sse.dispatchEvent(event);
    }

    const receivedEvents: TestEvent[] = [];

    await new Promise<void>((resolve) => {
      const es = createEventSource({
        url: "http://0.0.0.0/sse",
        fetch: (url) => {
          const req = new Request(url, {
            headers: {
              "Last-Event-ID": "1",
            },
          });
          return sse.fetch(req);
        },
        onMessage({ event, data }) {
          const reconstructedEvent = { ...JSON.parse(data), type: event };
          receivedEvents.push(reconstructedEvent);
          if (isEqual(reconstructedEvent, lastEvent)) {
            es.close();
            resolve();
          }
        },
      });
    });

    expect(receivedEvents).toEqual(events.slice(1));
  });

  it("should dispatch old events to EventSource with lastEventId query parameter", async () => {
    const eventStore = createEventStore();

    if (eventStore instanceof NullEventStore) {
      return;
    }

    const sse = new SSETarget("/sse", eventStore);

    // Dispatch events *before* the EventSource connects
    for (const event of events) {
      await sse.dispatchEvent(event);
    }

    const receivedEvents: TestEvent[] = [];

    await new Promise<void>((resolve) => {
      const es = createEventSource({
        // The native browser EventSource can't set a Last-Event-ID header on
        // the initial connection, so the resume point is passed in the query.
        url: "http://0.0.0.0/sse?lastEventId=1",
        fetch: (url) => {
          const req = new Request(url);
          return sse.fetch(req);
        },
        onMessage({ event, data }) {
          const reconstructedEvent = { ...JSON.parse(data), type: event };
          receivedEvents.push(reconstructedEvent);
          if (isEqual(reconstructedEvent, lastEvent)) {
            es.close();
            resolve();
          }
        },
      });
    });

    expect(receivedEvents).toEqual(events.slice(1));
  });
}
