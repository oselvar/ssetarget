# SSETarget

`@oselvar/ssetarget` dispatches Server-Sent Events to `EventSource` clients. It honours the `Last-Event-ID` header so reconnecting (or freshly connecting) clients resume from where they left off rather than missing past events.

## What it is

Conceptually: a typed `EventTarget` whose dispatches are written to a pluggable `EventStore` and exposed over HTTP using the SSE wire protocol. New or reconnecting clients receive previously stored events (subject to `Last-Event-ID`) before joining the live stream.

If you don't want any persistence, plug in `NullEventStore` and the behaviour collapses to plain `EventTarget` semantics — late listeners only see new events.

## Quick start

```ts
import { SSETarget, MemoryEventStore } from "@oselvar/ssetarget";

const sse = new SSETarget("/sse", new MemoryEventStore());

await sse.dispatchEvent({ type: "started" });

// Inside an HTTP handler that takes a `Request`:
function handleRequest(req: Request) {
  return sse.fetch(req);
}
```

Events must extend `ServerSentEvent` (`{ type: string; id?: number }`); the store assigns the `id`.

## Event stores

`SSETarget` is generic over an `EventStore`. The library ships three:

- **`MemoryEventStore`** (`@oselvar/ssetarget`) — keeps every event in memory. Good for a single-process server or local development.
- **`NullEventStore`** (`@oselvar/ssetarget`) — stores nothing. Reconnecting clients get only new events. Use this when you want pure `EventTarget` semantics.
- **`RedisEventStore`** (`@oselvar/ssetarget/redis`) — persists events in Redis with monotonic IDs. Good for horizontally scaled deployments.

```ts
import { Redis } from "ioredis";
import { SSETarget } from "@oselvar/ssetarget";
import { RedisEventStore } from "@oselvar/ssetarget/redis";

const sse = new SSETarget("/sse", new RedisEventStore(new Redis(), "my-prefix"));
```

Implement the `EventStore<E>` interface to plug in any other backing store.

## Cloudflare Workflows

The library was originally built to emit progress events from [Cloudflare Workflows](https://developers.cloudflare.com/workflows/), so `EventSource` clients can be notified as workflow steps start, complete, or fail.

The relevant pieces:

- `WorkflowEvents` (`@oselvar/ssetarget/workflows/cloudflare`) — a Durable Object that stores step events in SQLite and serves the SSE stream.
- `WorkflowEventStep` (`@oselvar/ssetarget/workflows/cloudflare`) — wraps a `WorkflowStep` so each `do` / `sleep` / `waitForEvent` call automatically dispatches `started`, `completed`, or `failed` events.
- `serveSSE` (`@oselvar/ssetarget/workflows/cloudflare/sse`) — small helper that routes an HTTP request to the right `WorkflowEvents` instance.

See `src/examples/` and `wrangler.toml` for a working setup.

### Try the example

    # Terminal 1
    npm start

Start a workflow (the `Location` header contains the instance id):

    curl -v -X POST http://localhost:9875

Listen to events from that workflow:

    curl http://localhost:9875/<instance-id>/sse

The workflow runs a few steps, emits SSEs, and pauses on a `waitForEvent`. Trigger it to make the workflow resume:

    curl -X POST http://localhost:9875/<instance-id>/event

## Prior art

The combination of "fan-out a stream" + "replay to late subscribers" appears in a few places, and `SSETarget` borrows shape from each:

- **DOM `EventTarget`** / **Node `EventEmitter`** — same dispatch shape, no persistence: late listeners miss prior events.
- **RxJS `ReplaySubject`** — the closest in-memory analog: a buffer is replayed to each new subscriber.
- **SSE `Last-Event-ID`** ([WHATWG spec](https://html.spec.whatwg.org/multipage/server-sent-events.html)) — the wire-protocol version of the same idea: when the browser reconnects, it sends the last `id:` it saw and the server resumes from there. `SSETarget` implements this; the `EventStore` is what makes resumption possible across process restarts.
- **Kafka**, **Redis Streams**, **NATS JetStream** — log-based message brokers with offset- or cursor-based replay.
- **Event Sourcing** — events as the durable source of truth from which state is derived.
