import { Hono } from "hono";

import { serveSSE } from "../workflows/cloudflare/sse/index.js";
export { WorkflowEvents } from "../workflows/cloudflare/WorkflowEvents.js";
export { DemoWorkflow } from "./DemoWorkflow.js";

const app = new Hono<{ Bindings: Env }>();

app.get("/:instanceId/sse", async (c) => {
  const instanceId = c.req.param("instanceId");
  const workflowEventsNs = c.env.WORKFLOW_EVENTS;
  return serveSSE(instanceId, c.req.raw, workflowEventsNs);
});

app.post("/:instanceId/event", async (c) => {
  const instanceId = c.req.param("instanceId");
  const workflow = c.env.DEMO_WORKFLOW;
  const instance = await workflow.get(instanceId);
  await instance.sendEvent({ type: "waiting", payload: "Wake up" });
  return new Response("Sent event\n", { status: 200 });
});

app.post("/", async (c) => {
  const workflow = c.env.DEMO_WORKFLOW;
  const { id } = await workflow.create();
  return Response.redirect(new URL(`/${id}/sse`, c.req.url), 302);
});

export default app;
