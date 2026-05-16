import { describe, expect, it } from "vitest";

import { MemoryEventStore } from "../MemoryEventStore.js";
import { SSETarget } from "../SSETarget.js";
import { type SpanEvent, Tracer } from "./index.js";

const TRACE_ID = "test-trace";

function makeTracer() {
  const eventStore = new MemoryEventStore<SpanEvent>();
  const target = new SSETarget<SpanEvent>("/sse", eventStore);
  const tracer = new Tracer((event) => target.dispatchEvent(event), TRACE_ID);
  return { eventStore, tracer };
}

async function recordedEvents(
  eventStore: MemoryEventStore<SpanEvent>,
): Promise<readonly SpanEvent[]> {
  return eventStore.getEvents(0);
}

describe("Tracer", () => {
  it("dispatches started then ended on success", async () => {
    const { eventStore, tracer } = makeTracer();

    const result = await tracer.withSpan(
      { spanId: "a", parentSpanId: null, name: "root" },
      async () => 42,
    );

    expect(result).toBe(42);

    const events = await recordedEvents(eventStore);
    expect(events).toHaveLength(2);
    expect(events[0]).toMatchObject({
      type: "started",
      traceId: TRACE_ID,
      spanId: "a",
      parentSpanId: null,
      name: "root",
      attributes: {},
    });
    expect(events[1]).toMatchObject({
      type: "ended",
      traceId: TRACE_ID,
      spanId: "a",
      status: { code: "OK" },
      attributes: {},
    });
  });

  it("dispatches ended with status ERROR and rethrows on error", async () => {
    const { eventStore, tracer } = makeTracer();

    await expect(
      tracer.withSpan({ spanId: "a", parentSpanId: null, name: "boom" }, async () => {
        throw new Error("nope");
      }),
    ).rejects.toThrow("nope");

    const events = await recordedEvents(eventStore);
    expect(events).toHaveLength(2);
    const ended = events[1] as Extract<SpanEvent, { type: "ended" }>;
    expect(ended).toMatchObject({
      type: "ended",
      traceId: TRACE_ID,
      spanId: "a",
      status: { code: "ERROR" },
    });
    expect(ended.status.code).toBe("ERROR");
    if (ended.status.code === "ERROR") {
      expect(ended.status.message).toContain("nope");
    }
  });

  it("orders nested spans: parent.started, child.started, child.ended, parent.ended", async () => {
    const { eventStore, tracer } = makeTracer();

    await tracer.withSpan({ spanId: "parent", parentSpanId: null, name: "root" }, async () => {
      await tracer.withSpan(
        { spanId: "child", parentSpanId: "parent", name: "inner" },
        async () => "ok",
      );
    });

    const events = await recordedEvents(eventStore);
    expect(events.map((e) => `${e.type}:${e.spanId}`)).toEqual([
      "started:parent",
      "started:child",
      "ended:child",
      "ended:parent",
    ]);
    for (const event of events) {
      expect(event.traceId).toBe(TRACE_ID);
    }
  });

  it("merges attributes set during the span into the ended event", async () => {
    const { eventStore, tracer } = makeTracer();

    await tracer.withSpan(
      {
        spanId: "a",
        parentSpanId: null,
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
    const { eventStore, tracer } = makeTracer();

    await tracer.withSpan({ spanId: "a", parentSpanId: null, name: "x" }, async (span) => {
      span.setAttributes({ added: "later" });
    });

    const events = await recordedEvents(eventStore);
    const started = events[0] as Extract<SpanEvent, { type: "started" }>;
    expect(started.attributes).toEqual({});
    const ended = events[1] as Extract<SpanEvent, { type: "ended" }>;
    expect(ended.attributes).toEqual({ added: "later" });
  });
});
