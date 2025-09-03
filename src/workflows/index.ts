export type StepEvent = {
  id?: number;
  type: "started" | "completed" | "failed";
  step: string;
  taskId: string;
  timestamp: string;
  error?: string;
};
