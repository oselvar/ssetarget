import type { JsonObject, SpanEvent, SpanHandle, SpanOptions } from "./index.js";

export type SpanEventSink = (event: SpanEvent) => Promise<void>;

export class Tracer {
  constructor(
    private readonly sink: SpanEventSink,
    private readonly traceId: string,
  ) {}

  async withSpan<T>(opts: SpanOptions, fn: (span: SpanHandle) => Promise<T>): Promise<T> {
    const attributes: Record<string, unknown> = { ...(opts.attributes ?? {}) };
    const handle: SpanHandle = {
      setAttributes(patch) {
        Object.assign(attributes, patch);
      },
    };

    await this.sink({
      type: "started",
      traceId: this.traceId,
      spanId: opts.spanId,
      parentSpanId: opts.parentSpanId,
      name: opts.name,
      attributes: { ...attributes } as JsonObject,
      timestamp: new Date().toISOString(),
    });

    try {
      const result = await fn(handle);
      await this.sink({
        type: "ended",
        traceId: this.traceId,
        spanId: opts.spanId,
        status: { code: "OK" },
        attributes: { ...attributes } as JsonObject,
        timestamp: new Date().toISOString(),
      });
      return result;
    } catch (err) {
      await this.sink({
        type: "ended",
        traceId: this.traceId,
        spanId: opts.spanId,
        status: { code: "ERROR", message: errorMessage(err) },
        attributes: { ...attributes } as JsonObject,
        timestamp: new Date().toISOString(),
      });
      throw err;
    }
  }
}

function errorMessage(err: unknown): string {
  if (err instanceof Error) {
    return err.stack ?? err.message;
  }
  return String(err);
}
