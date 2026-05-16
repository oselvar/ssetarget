import { WorkflowEntrypoint, type WorkflowEvent, type WorkflowStep } from "cloudflare:workers";

import { batchedDispatchEvent } from "../workflows/cloudflare/batchedDispatchEvent.js";
import { WorkflowEventStep } from "../workflows/cloudflare/WorkflowEventStep.js";

export class DemoWorkflow extends WorkflowEntrypoint<Env> {
  override async run(event: WorkflowEvent<unknown>, step: WorkflowStep) {
    const eventStep = new WorkflowEventStep(
      step,
      event.instanceId,
      batchedDispatchEvent(this.ctx, this.env.WORKFLOW_EVENTS, 5_000),
    );

    await eventStep.withWorkflow("DemoWorkflow", async () => {
      for (let i = 0; i < 20; i++) {
        if (i % 3 === 2) {
          const event = await eventStep.waitForEvent(`wait for event ${i}`, {
            type: "waiting",
            timeout: "1 minute",
          });
          // eslint-disable-next-line no-console
          console.log("Received event:", event);
        }
        await eventStep.sleep(`sleep_${i}`, "2 second");

        await eventStep.do(`step ${i}`, async () => {
          // eslint-disable-next-line no-console
          console.log(`Running step ${i}`);
        });
      }
    });
  }
}
