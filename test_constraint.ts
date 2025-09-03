import { type ServerSentEvent, type ServerSentEventWithId } from "./src/SSETarget";

// This should work - valid ServerSentEvent without id
type ValidEvent = ServerSentEvent & { data: string };
const validEvent: ServerSentEventWithId<ValidEvent> = {
  type: "message",
  data: "Hello world",
  id: 123,
};

// This should cause a compile error - type has an id field
type EventWithNumberId = ServerSentEvent & { id: number; data: string };
// This should fail because EventWithNumberId already has an id field
const invalidEvent: ServerSentEventWithId<EventWithNumberId> = {
  // @ts-expect-error - Should fail because EventWithNumberId already has an id field
  type: "test",
  // @ts-expect-error - Should fail because EventWithNumberId already has an id field
  data: "test",
  id: 1,
};

// Test with just the base ServerSentEvent type
const baseEvent: ServerSentEventWithId<ServerSentEvent> = {
  type: "basic",
  id: 42,
};

// eslint-disable-next-line no-console
console.log("Constraint tests completed!", validEvent, invalidEvent, baseEvent);
