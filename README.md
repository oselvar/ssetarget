# SSETarget

The `@oselvar/ssetarget` module exposes a simple API for dispatching Server-Sent Events to `EventSource` clients.
Events are persisted, allowing clients to receive previous events - with support for the `Last-Event-ID` header.

Dispatch events:

```typescript
const eventStore = new MemoryEventStore(); // Or use RedisEventStore
const sse = new SSETarget("/sse", eventStore);

await sse.dispatchEvent({
  type: "started",
});
```

Serve Server-Sent Events:

```typescript
function handleRequest(req: Request) {
  return sse.fetch(req);
}
```

## Cloudflare Workflow events

The library was originally developed to emit Server-Sent Events about the progress of [Cloudflare Workflows](https://developers.cloudflare.com/workflows/).

This allows `EventSource` clients to be notified as workflow steps are started and completed (or failed).

See the `./src/examples` directory and `./wrangler.toml` for details.

### Try out the example

Start the example:

    # Terminal 1
    npm start

Start a workflow:

    curl -v -X POST http://localhost:9875

Listen to events (using the ID from the `Location` header)

    curl http://localhost:9875/cbd3f7a0-e9ee-422d-8d33-9db7383dba2c/sse

The workflow will run some steps and emit SSEs, then wait for a workflow event.
Trigger one to make the workflow resume:

    curl -X POST http://localhost:9875/cbd3f7a0-e9ee-422d-8d33-9db7383dba2c/event
