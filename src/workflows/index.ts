export type JsonValue =
  | string
  | number
  | boolean
  | null
  | readonly JsonValue[]
  | { readonly [k: string]: JsonValue };
export type JsonObject = { readonly [k: string]: JsonValue };

export type SpanStatus =
  | { readonly code: "OK" }
  | { readonly code: "ERROR"; readonly message?: string };

export type SpanStartedEvent = {
  readonly id?: number;
  readonly type: "started";
  readonly traceId: string;
  readonly spanId: string;
  readonly parentSpanId: string | null;
  readonly name: string;
  readonly attributes: JsonObject;
  readonly timestamp: string;
};

export type SpanEndedEvent = {
  readonly id?: number;
  readonly type: "ended";
  readonly traceId: string;
  readonly spanId: string;
  readonly status: SpanStatus;
  readonly attributes: JsonObject;
  readonly timestamp: string;
};

export type SpanEvent = SpanStartedEvent | SpanEndedEvent;

export type SpanOptions = {
  readonly spanId: string;
  readonly parentSpanId: string | null;
  readonly name: string;
  readonly attributes?: JsonObject;
};

export interface SpanHandle {
  setAttributes(patch: JsonObject): void;
}

export { type SpanEventSink, Tracer } from "./Tracer.js";
