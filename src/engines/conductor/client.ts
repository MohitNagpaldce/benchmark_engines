export type ConductorWorkflowStatus =
  | "RUNNING"
  | "COMPLETED"
  | "FAILED"
  | "TERMINATED"
  | "TIMED_OUT"
  | "PAUSED";

export interface ConductorTask {
  taskId: string;
  workflowInstanceId: string;
  taskType: string;
  referenceTaskName: string;
  inputData?: Record<string, unknown>;
}

export interface ConductorWorkflowExecution {
  workflowId: string;
  status: ConductorWorkflowStatus;
  tasks?: Array<{
    taskType: string;
    status: string;
    referenceTaskName: string;
  }>;
}

export interface ConductorTaskUpdate {
  workflowInstanceId: string;
  taskId: string;
  status: "COMPLETED" | "FAILED" | "FAILED_WITH_TERMINAL_ERROR";
  outputData?: Record<string, unknown>;
  reasonForIncompletion?: string;
  callbackAfterSeconds?: number;
}

export class ConductorClient {
  constructor(private readonly baseUrl: string) {}

  async health(): Promise<void> {
    await this.request("/metadata/workflow", { method: "GET" });
  }

  async startWorkflow(body: Record<string, unknown>): Promise<string> {
    const response = await this.request("/workflow", {
      method: "POST",
      headers: { "content-type": "application/json", accept: "text/plain" },
      body: JSON.stringify(body)
    });
    return response.text();
  }

  async getWorkflow(workflowId: string, includeTasks = true): Promise<ConductorWorkflowExecution> {
    return this.requestJson<ConductorWorkflowExecution>(`/workflow/${workflowId}?includeTasks=${includeTasks}`);
  }

  async pollBatch(taskType: string, count: number, timeoutMs: number, workerId: string): Promise<ConductorTask[]> {
    const params = new URLSearchParams({
      count: String(count),
      timeout: String(timeoutMs),
      workerid: workerId
    });
    const response = await this.request(`/tasks/poll/batch/${taskType}?${params.toString()}`, {
      method: "GET"
    });

    if (response.status === 204) {
      return [];
    }

    return response.json() as Promise<ConductorTask[]>;
  }

  async updateTask(update: ConductorTaskUpdate): Promise<void> {
    await this.request("/tasks", {
      method: "POST",
      headers: { "content-type": "application/json", accept: "text/plain" },
      body: JSON.stringify(update)
    });
  }

  private async requestJson<T>(path: string): Promise<T> {
    const response = await this.request(path, { method: "GET" });
    return response.json() as Promise<T>;
  }

  private async request(path: string, init: RequestInit): Promise<Response> {
    const response = await fetch(`${this.baseUrl}${path}`, init);
    if (!response.ok) {
      const body = await response.text().catch(() => "");
      throw new Error(`Conductor request failed ${response.status} ${response.statusText}: ${body}`);
    }
    return response;
  }
}
