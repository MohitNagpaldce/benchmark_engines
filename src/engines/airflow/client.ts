export type AirflowDagRunState = "queued" | "running" | "success" | "failed";

export interface AirflowDagRun {
  dag_id: string;
  dag_run_id: string;
  state: AirflowDagRunState;
}

export class AirflowClient {
  constructor(
    private readonly baseUrl: string,
    private readonly username: string,
    private readonly password: string
  ) {}

  async health(): Promise<void> {
    await this.request("/health", { method: "GET" });
  }

  async triggerDagRun(dagId: string, dagRunId: string, conf: Record<string, unknown>): Promise<AirflowDagRun> {
    return this.requestJson<AirflowDagRun>(`/dags/${dagId}/dagRuns`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({
        dag_run_id: dagRunId,
        conf
      })
    });
  }

  async getDagRun(dagId: string, dagRunId: string): Promise<AirflowDagRun> {
    return this.requestJson<AirflowDagRun>(`/dags/${dagId}/dagRuns/${dagRunId}`, {
      method: "GET"
    });
  }

  async unpauseDag(dagId: string): Promise<void> {
    await this.request(`/dags/${dagId}`, {
      method: "PATCH",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ is_paused: false })
    });
  }

  private async requestJson<T>(path: string, init: RequestInit): Promise<T> {
    const response = await this.request(path, init);
    return response.json() as Promise<T>;
  }

  private async request(path: string, init: RequestInit): Promise<Response> {
    const auth = Buffer.from(`${this.username}:${this.password}`).toString("base64");
    const response = await fetch(`${this.baseUrl}${path}`, {
      ...init,
      headers: {
        accept: "application/json",
        authorization: `Basic ${auth}`,
        ...init.headers
      }
    });

    if (!response.ok) {
      const body = await response.text().catch(() => "");
      throw new Error(`Airflow request failed ${response.status} ${response.statusText}: ${body}`);
    }

    return response;
  }
}
