import { describe, expect, it } from "vitest";

import { MemoryEventStore } from "../MemoryEventStore.js";
import { SSETarget } from "../SSETarget.js";
import { type SpanEvent, WorkflowSpanDispatcher } from "./index.js";

function makeDispatcher() {
  const eventStore = new MemoryEventStore<SpanEvent>();
  const target = new SSETarget<SpanEvent>("/sse", eventStore);
  const dispatcher = new WorkflowSpanDispatcher((event) => target.dispatchEvent(event));
  return { eventStore, dispatcher };
}

async function recordedEvents(
  eventStore: MemoryEventStore<SpanEvent>,
): Promise<readonly SpanEvent[]> {
  return eventStore.getEvents(0);
}

describe("WorkflowSpanDispatcher", () => {
  it("dispatches started then ended on success", async () => {
    const { eventStore, dispatcher } = makeDispatcher();

    const result = await dispatcher.withSpan(
      { spanId: "a", parentSpanId: null, kind: "workflow", name: "root" },
      async () => 42,
    );

    expect(result).toBe(42);

    const events = await recordedEvents(eventStore);
    expect(events).toHaveLength(2);
    expect(events[0]).toMatchObject({
      type: "started",
      spanId: "a",
      parentSpanId: null,
      kind: "workflow",
      name: "root",
      attributes: {},
    });
    expect(events[1]).toMatchObject({
      type: "ended",
      spanId: "a",
      status: "completed",
      attributes: {},
    });
  });

  it("dispatches ended with status failed and rethrows on error", async () => {
    const { eventStore, dispatcher } = makeDispatcher();

    await expect(
      dispatcher.withSpan(
        { spanId: "a", parentSpanId: null, kind: "step", name: "boom" },
        async () => {
          throw new Error("nope");
        },
      ),
    ).rejects.toThrow("nope");

    const events = await recordedEvents(eventStore);
    expect(events).toHaveLength(2);
    expect(events[1]).toMatchObject({
      type: "ended",
      spanId: "a",
      status: "failed",
    });
    const ended = events[1] as Extract<SpanEvent, { type: "ended" }>;
    expect(ended.error).toContain("nope");
  });

  it("orders nested spans: parent.started, child.started, child.ended, parent.ended", async () => {
    const { eventStore, dispatcher } = makeDispatcher();

    await dispatcher.withSpan(
      { spanId: "parent", parentSpanId: null, kind: "workflow", name: "root" },
      async () => {
        await dispatcher.withSpan(
          { spanId: "child", parentSpanId: "parent", kind: "step", name: "inner" },
          async () => "ok",
        );
      },
    );

    const events = await recordedEvents(eventStore);
    expect(events.map((e) => `${e.type}:${e.spanId}`)).toEqual([
      "started:parent",
      "started:child",
      "ended:child",
      "ended:parent",
    ]);
  });

  it("merges attributes set during the span into the ended event", async () => {
    const { eventStore, dispatcher } = makeDispatcher();

    await dispatcher.withSpan(
      {
        spanId: "a",
        parentSpanId: null,
        kind: "step",
        name: "extract",
        attributes: { initial: true },
      },
      async (span) => {
        span.setAttributes({ files: ["a.txt"] });
        span.setAttributes({ count: 2 });
      },
    );

    const events = await recordedEvents(eventStore);
    expect(events[1]).toMatchObject({
      type: "ended",
      attributes: { initial: true, files: ["a.txt"], count: 2 },
    });
  });

  it("snapshots attributes at started so later mutations don't leak backwards", async () => {
    const { eventStore, dispatcher } = makeDispatcher();

    await dispatcher.withSpan(
      { spanId: "a", parentSpanId: null, kind: "step", name: "x" },
      async (span) => {
        span.setAttributes({ added: "later" });
      },
    );

    const events = await recordedEvents(eventStore);
    const started = events[0] as Extract<SpanEvent, { type: "started" }>;
    expect(started.attributes).toEqual({});
    const ended = events[1] as Extract<SpanEvent, { type: "ended" }>;
    expect(ended.attributes).toEqual({ added: "later" });
  });
});
