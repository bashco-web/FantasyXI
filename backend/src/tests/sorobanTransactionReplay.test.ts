import test from "node:test";
import assert from "node:assert/strict";
import {
  TransactionReplayWorker,
  TrackedTransaction,
  SorobanRpcClient,
  TransactionResubmitter,
} from "../services/financial/sorobanErrorRecovery.js";
import { eventBus } from "../services/events/eventBus.js";

test("Soroban Transaction Replay Daemon (Issue #211)", async (t) => {
  await t.test("Stuck transactions are detected and re-polled when older than pending timeout", async () => {
    const worker = new TransactionReplayWorker({ pendingTimeoutMs: 60000 });

    const now = new Date("2026-10-17T12:00:00.000Z");
    const freshSubmission = new Date("2026-10-17T11:59:30.000Z"); // 30s ago (not stuck)
    const stuckSubmission = new Date("2026-10-17T11:58:00.000Z"); // 120s ago (stuck)

    worker.trackTransaction({
      id: "tx-fresh",
      idempotencyKey: "idem-fresh",
      txHash: "hash-fresh",
      sourceAccount: "GADMIN",
      sequenceNumber: "1001",
      submittedAt: freshSubmission,
      payload: { intentType: "SETTLEMENT", leagueId: 101 },
    });

    worker.trackTransaction({
      id: "tx-stuck",
      idempotencyKey: "idem-stuck",
      txHash: "hash-stuck",
      sourceAccount: "GADMIN",
      sequenceNumber: "1002",
      submittedAt: stuckSubmission,
      payload: { intentType: "SETTLEMENT", leagueId: 102 },
    });

    const pending = worker.getPendingTransactions(now);
    assert.equal(pending.length, 1);
    assert.equal(pending[0].id, "tx-stuck");
  });

  await t.test("Idempotency keys prevent double submissions of identical payment/settlement intents", () => {
    const worker = new TransactionReplayWorker();

    const ok1 = worker.trackTransaction({
      id: "tx-1",
      idempotencyKey: "payout-intent-abc-123",
      txHash: "hash-1",
      sourceAccount: "GADMIN",
      sequenceNumber: "500",
      submittedAt: new Date(),
      payload: { intentType: "SETTLEMENT", leagueId: 200 },
    });
    assert.equal(ok1, true);
    assert.equal(worker.hasIdempotencyKey("payout-intent-abc-123"), true);

    // Duplicate intent with same idempotency key must be rejected
    const ok2 = worker.trackTransaction({
      id: "tx-2",
      idempotencyKey: "payout-intent-abc-123",
      txHash: "hash-2",
      sourceAccount: "GADMIN",
      sequenceNumber: "501",
      submittedAt: new Date(),
      payload: { intentType: "SETTLEMENT", leagueId: 200 },
    });
    assert.equal(ok2, false);
    assert.equal(worker.getAllTransactions().length, 1);
  });

  await t.test("Confirmed transaction is marked CONFIRMED and triggers post-settlement domain events", async () => {
    let domainEventReceived: any = null;
    let settlementConfirmedReceived: any = null;

    const un1 = eventBus.subscribe("TRANSACTION_CONFIRMED", (p) => {
      domainEventReceived = p;
    });
    const un2 = eventBus.subscribe("settlement.confirmed", (p) => {
      settlementConfirmedReceived = p;
    });

    try {
      const mockRpc: SorobanRpcClient = {
        async getTransaction(hash: string) {
          if (hash === "tx-hash-success") {
            return { status: "SUCCESS", ledger: 12345 };
          }
          return { status: "NOT_FOUND" };
        },
      };

      const worker = new TransactionReplayWorker({
        pendingTimeoutMs: 100,
        rpcClient: mockRpc,
      });

      const submittedAt = new Date(Date.now() - 5000);
      worker.trackTransaction({
        id: "tx-success",
        idempotencyKey: "idem-success",
        txHash: "tx-hash-success",
        sourceAccount: "GADMIN",
        sequenceNumber: "800",
        submittedAt,
        payload: { intentType: "SETTLEMENT", leagueId: 300 },
      });

      const pollResult = await worker.pollPendingTransactions();
      assert.deepEqual(pollResult.confirmed, ["tx-success"]);

      const tx = worker.getTransaction("tx-success");
      assert.equal(tx?.status, "CONFIRMED");

      assert.ok(domainEventReceived, "Expected TRANSACTION_CONFIRMED event");
      assert.equal(domainEventReceived.txId, "tx-success");
      assert.equal(domainEventReceived.idempotencyKey, "idem-success");

      assert.ok(settlementConfirmedReceived, "Expected settlement.confirmed event");
      assert.equal(settlementConfirmedReceived.leagueId, 300);
    } finally {
      un1();
      un2();
    }
  });

  await t.test("Mempool drop is detected (NOT_FOUND) and safely re-simulated & re-broadcast with incremented sequence", async () => {
    let replayEventReceived: any = null;
    const un = eventBus.subscribe("TRANSACTION_REPLAYED", (p) => {
      replayEventReceived = p;
    });

    try {
      // Simulate RPC returning NOT_FOUND (dropped from mempool during congestion)
      const mockRpc: SorobanRpcClient = {
        async getTransaction(hash: string) {
          return { status: "NOT_FOUND" };
        },
      };

      let replayedSequence = "1000";
      const mockResubmitter: TransactionResubmitter = {
        async resimulateAndBroadcast(tx: TrackedTransaction) {
          // Increment sequence number
          const nextSeq = (BigInt(tx.sequenceNumber) + 1n).toString();
          replayedSequence = nextSeq;
          return {
            txHash: `new-hash-for-${tx.id}-seq-${nextSeq}`,
            sequenceNumber: nextSeq,
          };
        },
      };

      const worker = new TransactionReplayWorker({
        pendingTimeoutMs: 100,
        rpcClient: mockRpc,
        resubmitter: mockResubmitter,
      });

      const submittedAt = new Date(Date.now() - 5000);
      worker.trackTransaction({
        id: "tx-dropped",
        idempotencyKey: "idem-drop-456",
        txHash: "stale-mempool-hash",
        sourceAccount: "GADMIN",
        sequenceNumber: "1000",
        submittedAt,
        payload: { intentType: "SETTLEMENT", leagueId: 400 },
      });

      const pollResult = await worker.pollPendingTransactions();
      assert.deepEqual(pollResult.replayed, ["tx-dropped"]);

      const tx = worker.getTransaction("tx-dropped");
      assert.equal(tx?.status, "PENDING"); // re-broadcast back to pending
      assert.equal(tx?.resubmissionCount, 1);
      assert.equal(tx?.sequenceNumber, "1001");
      assert.equal(tx?.txHash, "new-hash-for-tx-dropped-seq-1001");
      assert.equal(tx?.idempotencyKey, "idem-drop-456"); // idempotency key preserved!

      assert.ok(replayEventReceived, "Expected TRANSACTION_REPLAYED event");
      assert.equal(replayEventReceived.txId, "tx-dropped");
      assert.equal(replayEventReceived.newSequenceNumber, "1001");
      assert.equal(replayEventReceived.resubmissionCount, 1);
    } finally {
      un();
    }
  });

  await t.test("Worker start and stop timer lifecycle operates cleanly", () => {
    const worker = new TransactionReplayWorker({ pollIntervalMs: 200 });
    worker.start();
    worker.stop();
  });
});
