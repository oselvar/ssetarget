import { createEventSource } from "eventsource-client";
import { describe, expect, it } from "vitest";

import { MemorySSETarget } from "./MemorySSETarget";
import { type ServerSentEvent } from "./SSETarget";

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

describe("MemorySSETarget", () => {
  it("should dispatch events to Eventource", async () => {
    const sse = new MemorySSETarget<TestEvent>("/sse");

    for (const event of events) {
      sse.dispatchEvent(event);
    }

    const receivedEvents: TestEvent[] = [];

    await new Promise<void>((resolve, _reject) => {
      const es = createEventSource({
        url: "http://0.0.0.0/sse",
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

    expect(receivedEvents).toEqual(events);
  });

  it("should dispatch events to Eventource with lastEventId", async () => {
    const sse = new MemorySSETarget<TestEvent>("/sse");

    for (const event of events) {
      sse.dispatchEvent(event);
    }

    const receivedEvents: TestEvent[] = [];

    await new Promise<void>((resolve, _reject) => {
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
});

function isEqual(event1: TestEvent, event2: TestEvent): boolean {
  return event1.type === event2.type && event1.thing == event2.thing;
}
