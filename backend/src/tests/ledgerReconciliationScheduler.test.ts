import { describe, it } from "node:test";
import assert from "node:assert/strict";
import {
  LedgerReconciliationScheduler,
  ReconciliationAlert,
} from "../services/financial/financialService.js";
import {
  LeagueStatus,
  MembershipStatus,
  PaymentStatus,
  TransactionType,
  TransactionStatus,
} from "../types/index.js";
import { FinancialAuditAction } from "../services/audit/financialAuditLog.js";

const VALID_UUID_1 = "11111111-1111-1111-1111-111111111111";
const VALID_UUID_2 = "22222222-2222-2222-2222-222222222222";
const TEST_WALLET_1 = "GA2C5RFPE6GCKMY3US5PAB6UZLKIGAHWKXX2G6EXV2Z6QWQETVCH2D64";
const TEST_WALLET_2 = "GB2C5RFPE6GCKMY3US5PAB6UZLKIGAHWKXX2G6EXV2Z6QWQETVCH2D65";

function createMockSchedulerEnv() {
  const recordedAudits: any[] = [];
  const mockAudit = {
    record: (entry: any) => {
      recordedAudits.push(entry);
    },
  };

  const leaguesMap = new Map<string, any>();
  const mockDb = {
    league: {
      findUnique: async ({ where }: any) => leaguesMap.get(where.id) || null,
      findMany: async ({ where }: any) => {
        return Array.from(leaguesMap.values()).map((l) => ({ id: l.id }));
      },
    },
  };

  const contractStates = new Map<string, any>();
  const contractDeposits = new Map<string, bigint>();
  const contractTokenBalances = new Map<string, bigint>();

  const mockStellar: any = {
    getContractLeagueState: async (leagueId: bigint) => {
      return contractStates.get(leagueId.toString()) || null;
    },
    getContractDeposit: async (leagueId: bigint, address: string) => {
      const key = `${leagueId.toString()}:${address}`;
      return contractDeposits.get(key) || 0n;
    },
    getContractTokenBalance: async (tokenAddress: string) => {
      return contractTokenBalances.get(tokenAddress) || 0n;
    },
  };

  return {
    mockDb,
    leaguesMap,
    mockStellar,
    contractStates,
    contractDeposits,
    contractTokenBalances,
    mockAudit,
    recordedAudits,
  };
}

describe("LedgerReconciliationScheduler", () => {
  it("reconciles balanced league and logs BALANCED audit entry", async () => {
    const env = createMockSchedulerEnv();

    const leagueId = VALID_UUID_1;
    env.leaguesMap.set(leagueId, {
      id: leagueId,
      name: "Premier Champions",
      status: LeagueStatus.ACTIVE,
      members: [
        {
          id: "m1",
          userId: "u1",
          walletAddress: TEST_WALLET_1,
          paymentStatus: PaymentStatus.PAYMENT_CONFIRMED,
          status: MembershipStatus.ACTIVE,
        },
        {
          id: "m2",
          userId: "u2",
          walletAddress: TEST_WALLET_2,
          paymentStatus: PaymentStatus.PAYMENT_CONFIRMED,
          status: MembershipStatus.ACTIVE,
        },
      ],
      transactions: [
        {
          id: "tx1",
          type: TransactionType.ENTRY_FEE,
          status: TransactionStatus.CONFIRMED,
          amount: 25.0,
          userId: "u1",
        },
        {
          id: "tx2",
          type: TransactionType.ENTRY_FEE,
          status: TransactionStatus.CONFIRMED,
          amount: 25.0,
          userId: "u2",
        },
      ],
    });

    const contractLeagueId = BigInt(`0x${leagueId.replace(/-/g, "").slice(0, 16)}`);
    // 50 USDC = 500,000,000 stroops (7 decimals)
    env.contractStates.set(contractLeagueId.toString(), {
      league_id: contractLeagueId,
      total_deposited: 500_000_000n,
      participant_count: 2,
      token_address: "CTOKEN",
    });
    env.contractDeposits.set(`${contractLeagueId.toString()}:${TEST_WALLET_1}`, 250_000_000n);
    env.contractDeposits.set(`${contractLeagueId.toString()}:${TEST_WALLET_2}`, 250_000_000n);
    env.contractTokenBalances.set("CTOKEN", 500_000_000n);

    const scheduler = new LedgerReconciliationScheduler(
      env.mockDb,
      env.mockStellar,
      env.mockAudit
    );

    let alertFired = false;
    scheduler.onAlert(() => {
      alertFired = true;
    });

    const result = await scheduler.reconcileLeague(leagueId);

    assert.equal(result.status, "BALANCED");
    assert.equal(result.dbDepositsTotal, 50.0);
    assert.equal(result.onChainDepositedTotal, 50.0);
    assert.equal(result.discrepancy, 0);
    assert.equal(result.participantCountDb, 2);
    assert.equal(result.participantCountOnChain, 2);
    assert.equal(result.participantDiscrepancies.length, 0);
    assert.equal(alertFired, false);

    assert.equal(env.recordedAudits.length, 1);
    assert.equal(env.recordedAudits[0].action, FinancialAuditAction.RECONCILIATION_BALANCED);
    assert.equal(env.recordedAudits[0].metadata.status, "BALANCED");
  });

  it("detects discrepancy between on-chain contract and DB sum and emits alert", async () => {
    const env = createMockSchedulerEnv();

    const leagueId = VALID_UUID_2;
    env.leaguesMap.set(leagueId, {
      id: leagueId,
      name: "Drifting League",
      status: LeagueStatus.ACTIVE,
      members: [
        {
          id: "m1",
          userId: "u1",
          walletAddress: TEST_WALLET_1,
          paymentStatus: PaymentStatus.PAYMENT_CONFIRMED,
          status: MembershipStatus.ACTIVE,
        },
      ],
      transactions: [
        {
          id: "tx1",
          type: TransactionType.ENTRY_FEE,
          status: TransactionStatus.CONFIRMED,
          amount: 25.0,
          userId: "u1",
        },
      ],
    });

    const contractLeagueId = BigInt(`0x${leagueId.replace(/-/g, "").slice(0, 16)}`);
    // Contract has only 20 USDC deposited instead of 25 USDC (simulated discrepancy of 5 USDC)
    env.contractStates.set(contractLeagueId.toString(), {
      league_id: contractLeagueId,
      total_deposited: 200_000_000n,
      participant_count: 1,
    });
    env.contractDeposits.set(`${contractLeagueId.toString()}:${TEST_WALLET_1}`, 200_000_000n);

    const scheduler = new LedgerReconciliationScheduler(
      env.mockDb,
      env.mockStellar,
      env.mockAudit
    );

    const alerts: ReconciliationAlert[] = [];
    scheduler.onAlert((alert) => {
      alerts.push(alert);
    });

    const result = await scheduler.reconcileLeague(leagueId);

    assert.equal(result.status, "DISCREPANCY_DETECTED");
    assert.equal(result.dbDepositsTotal, 25.0);
    assert.equal(result.onChainDepositedTotal, 20.0);
    assert.equal(result.discrepancy, 5.0);
    assert.equal(result.participantDiscrepancies.length, 1);
    assert.equal(result.participantDiscrepancies[0].discrepancy, 5.0);

    // Verify alert emitted
    assert.equal(alerts.length, 1);
    assert.equal(alerts[0].type, "DISCREPANCY_DETECTED");
    assert.equal(alerts[0].leagueId, leagueId);
    assert.equal(alerts[0].discrepancy, 5.0);

    // Verify FinancialAuditLog entry
    assert.equal(env.recordedAudits.length, 1);
    assert.equal(
      env.recordedAudits[0].action,
      FinancialAuditAction.RECONCILIATION_DISCREPANCY
    );
    assert.equal(env.recordedAudits[0].metadata.status, "DISCREPANCY_DETECTED");
    assert.equal(env.recordedAudits[0].metadata.discrepancy, 5.0);
  });

  it("detects tiny drift > 0.000001 USDC and flags DISCREPANCY_DETECTED", async () => {
    const env = createMockSchedulerEnv();

    const leagueId = VALID_UUID_1;
    env.leaguesMap.set(leagueId, {
      id: leagueId,
      name: "Micro Drift League",
      status: LeagueStatus.ACTIVE,
      members: [],
      transactions: [
        {
          id: "tx1",
          type: TransactionType.ENTRY_FEE,
          status: TransactionStatus.CONFIRMED,
          amount: 10.0,
        },
      ],
    });

    const contractLeagueId = BigInt(`0x${leagueId.replace(/-/g, "").slice(0, 16)}`);
    // 10.000005 USDC -> difference is 0.000005 USDC (> 0.000001)
    env.contractStates.set(contractLeagueId.toString(), {
      league_id: contractLeagueId,
      total_deposited: 100_000_050n,
      participant_count: 1,
    });

    const scheduler = new LedgerReconciliationScheduler(
      env.mockDb,
      env.mockStellar,
      env.mockAudit
    );

    const result = await scheduler.reconcileLeague(leagueId);
    assert.equal(result.status, "DISCREPANCY_DETECTED");
    assert.ok(result.discrepancy > 0.000001);
  });

  it("runDailyReconciliation audits all active leagues and produces summary report", async () => {
    const env = createMockSchedulerEnv();

    // League 1: Balanced
    const l1 = VALID_UUID_1;
    env.leaguesMap.set(l1, {
      id: l1,
      name: "League 1",
      status: LeagueStatus.ACTIVE,
      members: [],
      transactions: [
        {
          id: "tx1",
          type: TransactionType.ENTRY_FEE,
          status: TransactionStatus.CONFIRMED,
          amount: 10.0,
        },
      ],
    });
    const c1 = BigInt(`0x${l1.replace(/-/g, "").slice(0, 16)}`);
    env.contractStates.set(c1.toString(), {
      league_id: c1,
      total_deposited: 100_000_000n,
      participant_count: 1,
    });

    // League 2: Mismatched
    const l2 = VALID_UUID_2;
    env.leaguesMap.set(l2, {
      id: l2,
      name: "League 2",
      status: LeagueStatus.ACTIVE,
      members: [],
      transactions: [
        {
          id: "tx2",
          type: TransactionType.ENTRY_FEE,
          status: TransactionStatus.CONFIRMED,
          amount: 20.0,
        },
      ],
    });
    const c2 = BigInt(`0x${l2.replace(/-/g, "").slice(0, 16)}`);
    env.contractStates.set(c2.toString(), {
      league_id: c2,
      total_deposited: 150_000_000n, // 15 USDC vs 20 USDC
      participant_count: 1,
    });

    const scheduler = new LedgerReconciliationScheduler(
      env.mockDb,
      env.mockStellar,
      env.mockAudit
    );

    const dailyReport = await scheduler.runDailyReconciliation();

    assert.equal(dailyReport.totalLeaguesAudited, 2);
    assert.equal(dailyReport.balancedCount, 1);
    assert.equal(dailyReport.discrepancyCount, 1);
    assert.ok(dailyReport.timestamp instanceof Date);
    assert.equal(dailyReport.results.length, 2);
  });

  it("scheduler timer starts and stops cleanly without leaving active handles", async () => {
    const env = createMockSchedulerEnv();
    const scheduler = new LedgerReconciliationScheduler(
      env.mockDb,
      env.mockStellar,
      env.mockAudit
    );

    scheduler.startDaily(100_000);
    // Safe to start again idempotently
    scheduler.startDaily(100_000);
    scheduler.stop();
    // Safe to stop again idempotently
    scheduler.stop();
  });
});
