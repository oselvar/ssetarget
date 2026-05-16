# SSETarget

`@oselvar/ssetarget` is a library for **live workflow observability over Server-Sent Events**. You instrument your workflows with hierarchical span events (modelled after [OpenTelemetry](https://opentelemetry.io/)), and the library streams them to `EventSource` clients with full replay support — late or reconnecting clients catch up via the `Last-Event-ID` header.

It works in two layers:

- **The SSE core** (`SSETarget` + `EventStore`) — a typed event-fanout primitive that persists every dispatched event and serves it over the SSE wire protocol. Useful on its own for any event-stream-to-browser use case.
- **The workflow layer** (`Tracer` + `WorkflowEventStep`) — a small, OTel-aligned span model on top of the core, with a ready-to-use bridge for [Cloudflare Workflows](https://developers.cloudflare.com/workflows/).

## Why spans?

Workflows are deeply nested: a workflow runs steps; steps can have iterations; iterations may include human-in-the-loop pauses. A flat list of "step started / step finished" loses that structure.

A span captures one unit of work, and every span knows its parent. That's enough to render a tree, compute durations, and roll up status — same idea OpenTelemetry uses for distributed tracing.

## Concepts

A span is two events on the wire: a `started` event when work begins and an `ended` event when it finishes (successfully or not).

```ts
type SpanStartedEvent = {
  type: "started";
  traceId: string; // groups spans belonging to the same workflow instance
  spanId: string; // unique id for this span
  parentSpanId: string | null; // null for the root span
  name: string;
  attributes: JsonObject;
  timestamp: string;
};

type SpanEndedEvent = {
  type: "ended";
  traceId: string;
  spanId: string;
  status: { code: "OK" } | { code: "ERROR"; message?: string };
  attributes: JsonObject;
  timestamp: string;
};
```

The field names mirror OpenTelemetry so you can write a thin adapter to OTLP if you ever need to forward spans to Jaeger, Tempo, Honeycomb, etc. The library itself has no OTel SDK dependency.

## Quick start (platform-agnostic)

Wrap async work in `tracer.withSpan(...)` and the library emits `started` + `ended` for you:

```ts
import { MemoryEventStore, SSETarget } from "@oselvar/ssetarget";
import { Tracer, type SpanEvent } from "@oselvar/ssetarget/workflows";

const sse = new SSETarget<SpanEvent>("/sse", new MemoryEventStore<SpanEvent>());
const tracer = new Tracer((event) => sse.dispatchEvent(event), "my-trace-id");

await tracer.withSpan({ spanId: "root", parentSpanId: null, name: "ingest" }, async (span) => {
  await tracer.withSpan({ spanId: "extract", parentSpanId: "root", name: "extract" }, async () => {
    // ...
  });
  span.setAttributes({ filesProcessed: 42 });
});
```

Inside the callback you get a `SpanHandle` whose `setAttributes(...)` patches attributes that show up on the `ended` event. The `started` event captures a snapshot at the moment the span begins.

## Streaming to clients

`SSETarget<SpanEvent>` is the HTTP-facing piece. Combine it with an `EventStore` to control persistence and replay behaviour:

- **`MemoryEventStore`** (`@oselvar/ssetarget`) — keeps every event in memory. Good for a single-process server or local development.
- **`NullEventStore`** (`@oselvar/ssetarget`) — stores nothing. Reconnecting clients only see new events. Use when you want pure `EventTarget` semantics.
- **`RedisEventStore`** (`@oselvar/ssetarget/redis`) — persists events in Redis with monotonic IDs. Good for horizontally scaled deployments.

```ts
import { Redis } from "ioredis";
import { SSETarget } from "@oselvar/ssetarget";
import { RedisEventStore } from "@oselvar/ssetarget/redis";

const sse = new SSETarget("/sse", new RedisEventStore(new Redis(), "my-prefix"));
```

Implement the `EventStore<E>` interface to plug in any other backing store.

## Cloudflare Workflows

The Cloudflare bridge wires up the tracer for you and emits a root span for the entire workflow instance:

- `WorkflowEvents` (`@oselvar/ssetarget/workflows/cloudflare`) — a Durable Object that persists span events in SQLite and serves the SSE stream.
- `WorkflowEventStep` (`@oselvar/ssetarget/workflows/cloudflare`) — wraps a `WorkflowStep`. `withWorkflow(name, fn)` emits the root span; every `do` / `sleep` / `sleepUntil` / `waitForEvent` call inside emits a child span.
- `serveSSE` (`@oselvar/ssetarget/workflows/cloudflare/sse`) — small helper that routes an HTTP request to the right `WorkflowEvents` instance.

```ts
import { WorkflowEntrypoint, type WorkflowEvent, type WorkflowStep } from "cloudflare:workers";
import { batchedDispatchEvent } from "@oselvar/ssetarget/workflows/cloudflare";
import { WorkflowEventStep } from "@oselvar/ssetarget/workflows/cloudflare";

export class MyWorkflow extends WorkflowEntrypoint<Env> {
  override async run(event: WorkflowEvent<unknown>, step: WorkflowStep) {
    const eventStep = new WorkflowEventStep(
      step,
      event.instanceId,
      batchedDispatchEvent(this.ctx, this.env.WORKFLOW_EVENTS, 5_000),
    );

    await eventStep.withWorkflow("MyWorkflow", async () => {
      await eventStep.do("step-1", async () => {
        /* ... */
      });
      await eventStep.sleep("wait", "5 second");
      await eventStep.do("step-2", async () => {
        /* ... */
      });
    });
  }
}
```

The root span's `spanId` is the workflow `instanceId`; child step spans use it as their `parentSpanId`. The `traceId` is the `instanceId` too, so all events from one workflow run are trivially grouped.

See `src/examples/` and `wrangler.toml` for a working setup.

### Try the example

    # Terminal 1
    pnpm start

Start a workflow (the `Location` header contains the instance id):

    curl -v -X POST http://localhost:9875

Listen to events from that workflow:

    curl http://localhost:9875/<instance-id>/sse

The workflow runs a few steps, emits spans, and pauses on a `waitForEvent`. Trigger it to make the workflow resume:

    curl -X POST http://localhost:9875/<instance-id>/event

## Mapping to OpenTelemetry

`SpanEvent` is an intentionally small subset of the OTel span shape — enough to render workflow trees in the browser, but easy to translate to OTLP if you want to export elsewhere.

| `SpanEvent` field                             | OpenTelemetry equivalent                                          |
| --------------------------------------------- | ----------------------------------------------------------------- |
| `traceId`, `spanId`, `parentSpanId`           | Same (you'd switch hex strings of fixed length for OTLP).         |
| `name`                                        | `Span.name`                                                       |
| `attributes`                                  | `Span.attributes` (use OTel semantic conventions where they fit). |
| `status: { code: "OK" \| "ERROR", message? }` | `Span.status` (no `UNSET` — spans are only emitted once ended).   |
| `started` event                               | `OnStart` processor callback.                                     |
| `ended` event                                 | `OnEnd` processor callback.                                       |

What this library deliberately **does not** ship: span links, intra-span timestamped events (`Span.addEvent(...)`), `SpanKind` (`INTERNAL`/`CLIENT`/`SERVER`/...), and any exporter. They're additive — write them when you need them.

## Migrating from 2.x

Version 3.0 replaces the flat `StepEvent` model with the hierarchical `SpanEvent` model.

| 2.x                                          | 3.x                                                                                         |
| -------------------------------------------- | ------------------------------------------------------------------------------------------- |
| `StepEvent`                                  | `SpanEvent` (`SpanStartedEvent \| SpanEndedEvent`)                                          |
| `type: "started" \| "completed" \| "failed"` | `type: "started" \| "ended"`, plus `status: { code: "OK" \| "ERROR", message? }` on `ended` |
| `taskId`, `step`                             | `spanId`, `name` (plus `traceId`, `parentSpanId`)                                           |
| `StepEventStore`                             | `SpanEventStore` (file renamed)                                                             |
| `WorkflowEventStep` only                     | `WorkflowEventStep` + new `withWorkflow(name, fn)` for the root span                        |
| (no equivalent)                              | New `Tracer` class for platform-agnostic span dispatch                                      |

If you have an existing UI consuming the 2.x events, the simplest migration is to update your event handler to switch on `type === "started"` vs `type === "ended"` and read `status.code` for success/failure.

## Prior art

The combination of "fan-out a stream" + "replay to late subscribers" appears in a few places, and `SSETarget` borrows shape from each:

- **DOM `EventTarget`** / **Node `EventEmitter`** — same dispatch shape, no persistence: late listeners miss prior events.
- **RxJS `ReplaySubject`** — the closest in-memory analog: a buffer is replayed to each new subscriber.
- **SSE `Last-Event-ID`** ([WHATWG spec](https://html.spec.whatwg.org/multipage/server-sent-events.html)) — the wire-protocol version of the same idea: when the browser reconnects, it sends the last `id:` it saw and the server resumes from there. `SSETarget` implements this; the `EventStore` is what makes resumption possible across process restarts.
- **Kafka**, **Redis Streams**, **NATS JetStream** — log-based message brokers with offset- or cursor-based replay.
- **Event Sourcing** — events as the durable source of truth from which state is derived.
- **OpenTelemetry** — the span/tracer model the workflow layer is built on; this library is intentionally a small subset for live in-browser observability.
