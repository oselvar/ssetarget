import { type ServerSentEvent } from "./SSETarget.js";

/**
 * Persists dispatched events so that SSETarget can replay events a client has
 * not yet received — on connect (`Last-Event-ID` header / `lastEventId` query
 * parameter) and when a slow connection's event queue overflows and dropped
 * events must be re-read from the store.
 *
 * SSETarget's delivery guarantees rest on this contract:
 *
 * - `storeEvent` returns the event with a numeric `id` that is strictly
 *   greater than every previously assigned id (monotonically increasing).
 * - Once `storeEvent` has resolved, the event is visible to every subsequent
 *   `getEvents` call. (SSETarget only notifies connections after `storeEvent`
 *   resolves, and deduplicates events that appear in both a replay and a
 *   connection's live queue by id.)
 * - `getEvents(lastEventId)` returns all stored events with
 *   `id > lastEventId`, in ascending id order.
 *
 * A store that does not assign ids (like NullEventStore) must return no
 * events from `getEvents`. Such a store provides no replay: clients only
 * receive events dispatched while they are connected, and events dropped when
 * a connection's queue overflows are lost for that connection.
 */
export interface EventStore<E extends ServerSentEvent> {
  storeEvent(event: E): Promise<E>;
  getEvents(lastEventId: number): Promise<readonly E[]>;
}
