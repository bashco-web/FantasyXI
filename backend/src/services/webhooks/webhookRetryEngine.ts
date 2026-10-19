/**
 * Webhook Delivery Retry Engine with Dead-Letter Queue (DLQ) & Subscription Circuit (Issue #219).
 *
 * Implements:
 * - Deterministic exponential backoff schedule: immediate, 1m, 5m, 15m, 1h (up to 5 attempts)
 * - Dead-letter queue tracking for deliveries failing all 5 attempts
 * - Automatic subscription deactivation after 10 consecutive delivery failures
 * - Replay fidelity: preserves original payload and generates fresh valid signature on manual replay
 * - Delivery attempt history tracking: status, statusCode, latencyMs, timestamp
 */

import { generateWebhookSignature } from "./webhookSigner.js";
import { WebhookEndpoint } from "./webhookDispatcher.js";

export const RETRY_SCHEDULE_MS = [
  0,          // Attempt 1: immediate
  60_000,     // Attempt 2: 1 minute
  300_000,    // Attempt 3: 5 minutes
  900_000,    // Attempt 4: 15 minutes
  3_600_000,  // Attempt 5: 1 hour
];

export const MAX_RETRY_ATTEMPTS = 5;
export const MAX_CONSECUTIVE_FAILURES_BEFORE_DEACTIVATION = 10;

export interface WebhookDeliveryAttempt {
  attemptNumber: number;
  status: "SUCCESS" | "FAILED";
  statusCode?: number | null;
  latencyMs: number;
  error?: string | null;
  responseBody?: string | null;
  timestamp: Date;
}

export interface RetryDeliveryRecord {
  id: string;
  endpointId: string;
  event: string;
  payload: Record<string, unknown>;
  status: "PENDING" | "DELIVERED" | "FAILED" | "DEAD_LETTER";
  attemptCount: number;
  maxAttempts: number;
  attempts: WebhookDeliveryAttempt[];
  nextRetryAt: Date | null;
  createdAt: Date;
  deliveredAt?: Date | null;
  lastError?: string | null;
}

export interface RetryEngineOptions {
  timeoutMs?: number;
  httpFetch?: (url: string, init: RequestInit) => Promise<Response>;
}

export function calculateBackoffDelay(attemptNumber: number): number {
  if (attemptNumber <= 1) return RETRY_SCHEDULE_MS[0];
  const index = Math.min(attemptNumber - 1, RETRY_SCHEDULE_MS.length - 1);
  return RETRY_SCHEDULE_MS[index];
}

export class WebhookRetryEngine {
  private timeoutMs: number;
  private httpFetch: (url: string, init: RequestInit) => Promise<Response>;

  private deliveries: Map<string, RetryDeliveryRecord> = new Map();
  private endpoints: Map<string, WebhookEndpoint> = new Map();
  private consecutiveFailures: Map<string, number> = new Map();

  constructor(options: RetryEngineOptions = {}) {
    this.timeoutMs = options.timeoutMs ?? 10_000;
    this.httpFetch = options.httpFetch ?? (globalThis.fetch as any);
  }

  public registerEndpoint(endpoint: WebhookEndpoint): void {
    this.endpoints.set(endpoint.id, endpoint);
    if (!this.consecutiveFailures.has(endpoint.id)) {
      this.consecutiveFailures.set(endpoint.id, 0);
    }
  }

  public getEndpoint(id: string): WebhookEndpoint | undefined {
    return this.endpoints.get(id);
  }

  public getAllEndpoints(): WebhookEndpoint[] {
    return Array.from(this.endpoints.values());
  }

  public getConsecutiveFailures(endpointId: string): number {
    return this.consecutiveFailures.get(endpointId) ?? 0;
  }

  public reactivateEndpoint(endpointId: string): WebhookEndpoint {
    const endpoint = this.endpoints.get(endpointId);
    if (!endpoint) {
      throw new Error(`Endpoint not found: ${endpointId}`);
    }
    endpoint.isActive = true;
    this.consecutiveFailures.set(endpointId, 0);
    console.log(`[WebhookRetryEngine] Reactivated webhook endpoint: ${endpointId}`);
    return endpoint;
  }

  public async dispatch(
    endpoint: WebhookEndpoint,
    event: string,
    payload: Record<string, unknown>,
    deliveryId?: string
  ): Promise<RetryDeliveryRecord> {
    this.registerEndpoint(endpoint);

    const id = deliveryId ?? `del_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`;
    const record: RetryDeliveryRecord = {
      id,
      endpointId: endpoint.id,
      event,
      payload,
      status: "PENDING",
      attemptCount: 0,
      maxAttempts: MAX_RETRY_ATTEMPTS,
      attempts: [],
      nextRetryAt: null,
      createdAt: new Date(),
    };

    this.deliveries.set(id, record);
    return await this.executeAttempt(record, endpoint);
  }

  public async executeAttempt(
    record: RetryDeliveryRecord,
    endpoint: WebhookEndpoint
  ): Promise<RetryDeliveryRecord> {
    if (!endpoint.isActive) {
      record.status = "DEAD_LETTER";
      record.lastError = "Endpoint is DEACTIVATED due to excessive consecutive delivery failures";
      record.nextRetryAt = null;
      return record;
    }

    record.attemptCount++;
    const attemptNumber = record.attemptCount;
    const payloadStr = JSON.stringify(record.payload);
    const { header } = generateWebhookSignature(payloadStr, endpoint.secret);

    const startedAt = performance.now();
    let responseStatus: number | null = null;
    let responseBody: string | null = null;
    let errorMsg: string | null = null;
    let isSuccess = false;

    try {
      const controller = new AbortController();
      const timer = setTimeout(() => controller.abort(), this.timeoutMs);

      const response = await this.httpFetch(endpoint.url, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "X-FantasyXI-Signature": header,
          "X-FantasyXI-Event": record.event,
          "X-FantasyXI-Delivery": record.id,
          "X-FantasyXI-Attempt": String(attemptNumber),
          "User-Agent": "FantasyXI-Webhook-Dispatcher/1.0",
        },
        body: payloadStr,
        signal: controller.signal,
      });

      clearTimeout(timer);
      responseStatus = response.status;
      const text = await response.text().catch(() => "");
      responseBody = text.slice(0, 500);

      if (response.ok) {
        isSuccess = true;
      } else {
        errorMsg = `HTTP Error: ${response.status} ${response.statusText}`;
      }
    } catch (err: any) {
      errorMsg = err.message || "Network / Dispatch error";
    }

    const latencyMs = Math.round((performance.now() - startedAt) * 100) / 100;
    const attempt: WebhookDeliveryAttempt = {
      attemptNumber,
      status: isSuccess ? "SUCCESS" : "FAILED",
      statusCode: responseStatus,
      latencyMs,
      error: errorMsg,
      responseBody,
      timestamp: new Date(),
    };

    record.attempts.push(attempt);
    record.lastError = errorMsg;

    if (isSuccess) {
      record.status = "DELIVERED";
      record.deliveredAt = new Date();
      record.nextRetryAt = null;
      // Reset consecutive failure counter on success
      this.consecutiveFailures.set(endpoint.id, 0);
    } else {
      // Increment consecutive failures for this endpoint
      const currentFailures = (this.consecutiveFailures.get(endpoint.id) ?? 0) + 1;
      this.consecutiveFailures.set(endpoint.id, currentFailures);

      // Check for subscription auto-deactivation after 10 consecutive failures
      if (currentFailures >= MAX_CONSECUTIVE_FAILURES_BEFORE_DEACTIVATION) {
        endpoint.isActive = false;
        console.warn(
          `🚨 Endpoint ${endpoint.id} DEACTIVATED after ${currentFailures} consecutive delivery failures!`
        );
      }

      if (attemptNumber >= record.maxAttempts) {
        record.status = "DEAD_LETTER";
        record.nextRetryAt = null;
        console.warn(
          `💀 Webhook delivery ${record.id} moved to DEAD-LETTER QUEUE after ${attemptNumber} failed attempts.`
        );
      } else {
        record.status = "FAILED";
        const delayMs = calculateBackoffDelay(attemptNumber + 1);
        record.nextRetryAt = new Date(Date.now() + delayMs);
      }
    }

    this.deliveries.set(record.id, record);
    return record;
  }

  public getDelivery(id: string): RetryDeliveryRecord | undefined {
    return this.deliveries.get(id);
  }

  public getDeadLetterQueue(endpointId?: string): RetryDeliveryRecord[] {
    const dlq = Array.from(this.deliveries.values()).filter(
      (d) => d.status === "DEAD_LETTER"
    );
    if (endpointId) {
      return dlq.filter((d) => d.endpointId === endpointId);
    }
    return dlq;
  }

  public listDeliveries(endpointId?: string): RetryDeliveryRecord[] {
    const all = Array.from(this.deliveries.values());
    if (endpointId) {
      return all.filter((d) => d.endpointId === endpointId);
    }
    return all;
  }

  /**
   * Manually replays a failed or dead-letter webhook delivery with full payload and signature fidelity.
   */
  public async retryDelivery(deliveryId: string): Promise<RetryDeliveryRecord> {
    const record = this.deliveries.get(deliveryId);
    if (!record) {
      throw new Error(`Delivery record not found: ${deliveryId}`);
    }

    const endpoint = this.endpoints.get(record.endpointId);
    if (!endpoint) {
      throw new Error(`Webhook endpoint not found for delivery: ${record.endpointId}`);
    }

    console.log(`🔄 Replaying webhook delivery ${deliveryId} to endpoint ${endpoint.id}...`);
    // Replay executes a new delivery attempt while preserving payload
    return await this.executeAttempt(record, endpoint);
  }
}

export const webhookRetryEngine = new WebhookRetryEngine();
