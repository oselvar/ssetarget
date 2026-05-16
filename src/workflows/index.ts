export type SpanKind = "workflow" | "step" | "iteration" | "hitl";

export type SpanStartedEvent = {
  readonly id?: number;
  readonly type: "started";
  readonly spanId: string;
  readonly parentSpanId: string | null;
  readonly kind: SpanKind;
  readonly name: string;
  readonly attributes: Readonly<Record<string, unknown>>;
  readonly timestamp: string;
};

export type SpanEndedEvent = {
  readonly id?: number;
  readonly type: "ended";
  readonly spanId: string;
  readonly status: "completed" | "failed";
  readonly attributes: Readonly<Record<string, unknown>>;
  readonly error?: string;
  readonly timestamp: string;
};

export type SpanEvent = SpanStartedEvent | SpanEndedEvent;

export type SpanOptions = {
  readonly spanId: string;
  readonly parentSpanId: string | null;
  readonly kind: SpanKind;
  readonly name: string;
  readonly attributes?: Record<string, unknown>;
};

export interface SpanHandle {
  setAttributes(patch: Record<string, unknown>): void;
}

export { type SpanEventSink, WorkflowSpanDispatcher } from "./WorkflowSpanDispatcher.js";
