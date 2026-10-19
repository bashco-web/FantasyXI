import { Request, Response, NextFunction } from "express";
import {
  cryptoAuditLog,
  VerificationResult,
  CryptoAuditEntry,
} from "../services/audit/cryptoAuditLog.js";
import {
  securityAnomalyLog,
  SecurityAnomalyType,
} from "../services/security/securityAnomalyLog.js";
import { eventBus } from "../services/events/eventBus.js";
import {
  SECURITY_TAMPER_DETECTED_EVENT,
  SecurityTamperDetectedPayload,
} from "../services/events/domainEvents.js";

/**
 * Cryptographic Audit Controller (Issue #212)
 *
 * Exposes authenticated admin verification endpoints and scheduled verification
 * workers detecting historical record alterations or chain breaks.
 */

export async function verifyAuditChainHandler(
  req: Request,
  res: Response,
  next: NextFunction
): Promise<void> {
  try {
    const result = verifyAuditChainAndAlert(req.ip || "127.0.0.1");

    if (!result.valid) {
      res.status(409).json({
        success: false,
        valid: false,
        verifiedBlocks: result.verifiedBlocks ?? 0,
        tamperedIndex: result.tamperedIndex,
        corruptRecordId: result.corruptRecordId,
        error: result.error,
      });
      return;
    }

    res.json({
      success: true,
      valid: true,
      verifiedBlocks: result.verifiedBlocks ?? result.totalEntries,
      totalEntries: result.totalEntries,
      lastVerifiedHash: result.lastVerifiedHash,
    });
  } catch (error) {
    next(error);
  }
}

/**
 * Verifies audit log chain, logs to securityAnomalyLog, and emits domain event on tamper.
 */
export function verifyAuditChainAndAlert(
  ip: string = "127.0.0.1",
  customEntries?: CryptoAuditEntry[]
): VerificationResult {
  const result = cryptoAuditLog.verify(customEntries);

  if (!result.valid) {
    securityAnomalyLog.record({
      type: SecurityAnomalyType.SECURITY_TAMPER_DETECTED,
      identifier: result.corruptRecordId || "audit_chain",
      ip,
      detail: {
        error: result.error,
        tamperedIndex: result.tamperedIndex,
        corruptRecordId: result.corruptRecordId,
        totalEntries: result.totalEntries,
      },
    });

    const payload: SecurityTamperDetectedPayload = {
      corruptRecordId: result.corruptRecordId,
      tamperedIndex: result.tamperedIndex,
      totalEntries: result.totalEntries,
      error: result.error,
      detectedAt: new Date().toISOString(),
    };

    eventBus.publish(SECURITY_TAMPER_DETECTED_EVENT, payload);
  }

  return result;
}

/**
 * Hourly verification background worker.
 */
export class AuditVerificationWorker {
  private timer: NodeJS.Timeout | null = null;
  private isRunning: boolean = false;

  constructor(private readonly intervalMs: number = 3600000) {}

  public start(): void {
    if (this.isRunning) return;
    this.isRunning = true;
    this.timer = setInterval(() => {
      this.runVerification();
    }, this.intervalMs);
    if (this.timer && typeof this.timer.unref === "function") {
      this.timer.unref();
    }
  }

  public stop(): void {
    if (this.timer) {
      clearInterval(this.timer);
      this.timer = null;
    }
    this.isRunning = false;
  }

  public runVerification(): VerificationResult {
    return verifyAuditChainAndAlert("system-worker");
  }
}

export const auditVerificationWorker = new AuditVerificationWorker();
