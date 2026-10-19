import test from "node:test";
import assert from "node:assert/strict";
import {
  CryptographicAuditLog,
  CryptoAuditEntry,
  GENESIS_HASH,
} from "../services/audit/cryptoAuditLog.js";
import {
  verifyAuditChainAndAlert,
  AuditVerificationWorker,
} from "../controllers/audit.controller.js";
import {
  securityAnomalyLog,
  SecurityAnomalyType,
} from "../services/security/securityAnomalyLog.js";
import { eventBus } from "../services/events/eventBus.js";
import { SECURITY_TAMPER_DETECTED_EVENT } from "../services/events/domainEvents.js";

test("Audit Hash Chain Verification (Issue #212)", async (t) => {
  t.beforeEach(() => {
    securityAnomalyLog.clear();
  });

  await t.test("Verification scans all audit rows and returns valid: true with verified block count", () => {
    const logger = new CryptographicAuditLog();

    logger.record("USDC_DEPOSIT", "user_1", "wallet", "w_1", { amount: "100" });
    logger.record("SQUAD_TRANSFER", "user_1", "squad", "sq_1", { transferIn: 201, transferOut: 101 });
    logger.record("LEAGUE_UPDATE", "admin_1", "league", "lg_1", { entryFee: "20" });

    const chain = logger.getChain();
    const result = logger.verify(chain);

    assert.equal(result.valid, true);
    assert.equal(result.totalEntries, 3);
    assert.equal(result.verifiedBlocks, 3);
    assert.equal(typeof result.lastVerifiedHash, "string");
  });

  await t.test("Tampering with historical audit row payload flags specific corrupt record ID", () => {
    const logger = new CryptographicAuditLog();

    const e1 = logger.record("USDC_DEPOSIT", "user_1", "wallet", "w_1", { amount: "100" });
    const e2 = logger.record("SQUAD_TRANSFER", "user_1", "squad", "sq_1", { transferIn: 201, transferOut: 101 });
    const e3 = logger.record("LEAGUE_UPDATE", "admin_1", "league", "lg_1", { entryFee: "20" });

    const chain: CryptoAuditEntry[] = JSON.parse(JSON.stringify(logger.getChain()));

    // Tamper with e2 payload
    (chain[1].payload as any).transferIn = 999;

    const result = logger.verify(chain);
    assert.equal(result.valid, false);
    assert.equal(result.tamperedIndex, 1);
    assert.equal(result.corruptRecordId, e2.id);
    assert.match(result.error!, /Tampered row detected at index 1/);
  });

  await t.test("Detects deleted row resulting in sequence and hash continuity mismatch", () => {
    const logger = new CryptographicAuditLog();

    const e1 = logger.record("USDC_DEPOSIT", "user_1", "wallet", "w_1", { amount: "100" });
    const e2 = logger.record("SQUAD_TRANSFER", "user_1", "squad", "sq_1", { transferIn: 201, transferOut: 101 });
    const e3 = logger.record("LEAGUE_UPDATE", "admin_1", "league", "lg_1", { entryFee: "20" });

    const chain: CryptoAuditEntry[] = JSON.parse(JSON.stringify(logger.getChain()));

    // Delete e2 (index 1)
    chain.splice(1, 1);

    const result = logger.verify(chain);
    assert.equal(result.valid, false);
    assert.equal(result.tamperedIndex, 1);
    assert.equal(result.corruptRecordId, e3.id);
    assert.match(result.error!, /Sequence mismatch at index 1/);
  });

  await t.test("Detects altered hash and broken previousHash links", () => {
    const logger = new CryptographicAuditLog();

    const e1 = logger.record("USDC_DEPOSIT", "user_1", "wallet", "w_1", { amount: "100" });
    const e2 = logger.record("SQUAD_TRANSFER", "user_1", "squad", "sq_1", { transferIn: 201, transferOut: 101 });

    const chain: CryptoAuditEntry[] = JSON.parse(JSON.stringify(logger.getChain()));

    // Corrupt previousHash on e2
    chain[1].previousHash = "deadbeef".repeat(8);

    const result = logger.verify(chain);
    assert.equal(result.valid, false);
    assert.equal(result.tamperedIndex, 1);
    assert.equal(result.corruptRecordId, e2.id);
    assert.match(result.error!, /Broken hash continuity/);
  });

  await t.test("verifyAuditChainAndAlert records anomaly in securityAnomalyLog and emits domain event", async () => {
    let eventReceived: any = null;
    const unsubscribe = eventBus.subscribe(SECURITY_TAMPER_DETECTED_EVENT, (payload) => {
      eventReceived = payload;
    });

    try {
      const logger = new CryptographicAuditLog();
      logger.record("USDC_DEPOSIT", "user_1", "wallet", "w_1", { amount: "100" });
      logger.record("SQUAD_TRANSFER", "user_1", "squad", "sq_1", { transferIn: 201, transferOut: 101 });

      const tamperedChain: CryptoAuditEntry[] = JSON.parse(JSON.stringify(logger.getChain()));
      (tamperedChain[1].payload as any).transferIn = 777;

      const result = verifyAuditChainAndAlert("192.168.1.100", tamperedChain);
      assert.equal(result.valid, false);

      // Verify securityAnomalyLog received the alert
      const anomalies = securityAnomalyLog.query({ type: SecurityAnomalyType.SECURITY_TAMPER_DETECTED });
      assert.equal(anomalies.length, 1);
      assert.equal(anomalies[0].type, SecurityAnomalyType.SECURITY_TAMPER_DETECTED);
      assert.equal(anomalies[0].identifier, tamperedChain[1].id);
      assert.equal(anomalies[0].ip, "192.168.1.100");

      // Verify domain event was emitted on eventBus
      assert.ok(eventReceived, "Expected domain event to be published");
      assert.equal(eventReceived.corruptRecordId, tamperedChain[1].id);
      assert.equal(eventReceived.tamperedIndex, 1);
    } finally {
      unsubscribe();
    }
  });

  await t.test("AuditVerificationWorker runs verification without errors and stops cleanly", () => {
    const worker = new AuditVerificationWorker(500);
    worker.start();
    const result = worker.runVerification();
    assert.equal(result.valid, true);
    worker.stop();
  });
});
