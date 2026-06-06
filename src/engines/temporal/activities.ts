import { Context } from "@temporalio/activity";

export interface MockRpcInput {
  stepName: string;
  latencyMs: number;
  payloadBytes?: number;
  failureRatePercent?: number;
}

export interface MockRpcResult {
  stepName: string;
  latencyMs: number;
  payloadBytes: number;
  attempt: number;
}

export async function mockRpc(input: MockRpcInput): Promise<MockRpcResult> {
  const latencyMs = Math.max(0, input.latencyMs);
  if (latencyMs > 0) {
    await new Promise((resolve) => setTimeout(resolve, latencyMs));
  }

  const failureRate = Math.max(0, Math.min(100, input.failureRatePercent ?? 0));
  if (failureRate > 0 && Math.random() * 100 < failureRate) {
    throw new Error(`Injected transient failure in ${input.stepName}`);
  }

  return {
    stepName: input.stepName,
    latencyMs,
    payloadBytes: input.payloadBytes ?? 0,
    attempt: Context.current().info.attempt
  };
}
