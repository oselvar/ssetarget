import type { SpanEvent, SpanHandle, SpanOptions } from "./index.js";

export type SpanEventSink = (event: SpanEvent) => Promise<void>;

export class WorkflowSpanDispatcher {
  constructor(private readonly sink: SpanEventSink) {}

  async withSpan<T>(
    opts: SpanOptions,
    fn: (span: SpanHandle) => Promise<T>,
  ): Promise<T> {
    const attributes: Record<string, unknown> = { ...(opts.attributes ?? {}) };
    const handle: SpanHandle = {
      setAttributes(patch) {
        Object.assign(attributes, patch);
      },
    };

    await this.sink({
      type: "started",
      spanId: opts.spanId,
      parentSpanId: opts.parentSpanId,
      kind: opts.kind,
      name: opts.name,
      attributes: { ...attributes },
      timestamp: new Date().toISOString(),
    });

    try {
      const result = await fn(handle);
      await this.sink({
        type: "ended",
        spanId: opts.spanId,
        status: "completed",
        attributes: { ...attributes },
        timestamp: new Date().toISOString(),
      });
      return result;
    } catch (err) {
      await this.sink({
        type: "ended",
        spanId: opts.spanId,
        status: "failed",
        attributes: { ...attributes },
        error: errorMessage(err),
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
