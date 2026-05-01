import { WorkflowEntrypoint, type WorkflowEvent, type WorkflowStep } from "cloudflare:workers";

import { batchedDispatchEvent } from "../workflows/cloudflare/batchedDispatchEvent.js";
import { WorkflowEventStep } from "../workflows/cloudflare/WorkflowEventStep.js";

export class DemoWorkflow extends WorkflowEntrypoint<Env> {
  override async run(event: WorkflowEvent<unknown>, step: WorkflowStep) {
    step = new WorkflowEventStep(
      step,
      event.instanceId,
      batchedDispatchEvent(this.ctx, this.env.WORKFLOW_EVENTS, 5_000),
    );

    for (let i = 0; i < 20; i++) {
      if (i % 3 === 2) {
        const event = await step.waitForEvent(`wait for event ${i}`, {
          type: "waiting",
          timeout: "1 minute",
        });
        // eslint-disable-next-line no-console
        console.log("Received event:", event);
      }
      await step.sleep(`sleep_${i}`, "2 second");

      await step.do(`step ${i}`, async () => {
        // eslint-disable-next-line no-console
        console.log(`Running step ${i}`);
      });
    }
  }
}
