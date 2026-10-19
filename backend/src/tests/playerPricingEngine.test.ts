import { describe, it } from "node:test";
import assert from "node:assert/strict";
import type { PrismaClient } from "@prisma/client";
import {
  PlayerPricingEngine,
  calculatePriceAdjustment,
  MIN_PLAYER_PRICE,
  MAX_PLAYER_PRICE,
  MAX_PRICE_CHANGE_PER_24H,
} from "../services/fpl/playerPricingEngine.js";

describe("PlayerPricingEngine - Dynamic Player Valuation", () => {
  describe("Pricing Formula and Boundary Calculations", () => {
    it("increases price by £0.1m when net transfers meet positive threshold", () => {
      const result = calculatePriceAdjustment(8.0, 10, { threshold: 5 });
      assert.equal(result.newPrice, 8.1);
      assert.equal(result.diff, 0.1);
      assert.equal(result.adjusted, true);
      assert.ok(result.reason.includes("Heavy net transfers in"));
    });

    it("decreases price by £0.1m when net transfers meet negative threshold", () => {
      const result = calculatePriceAdjustment(8.0, -10, { threshold: 5 });
      assert.equal(result.newPrice, 7.9);
      assert.equal(result.diff, -0.1);
      assert.equal(result.adjusted, true);
      assert.ok(result.reason.includes("Heavy net transfers out"));
    });

    it("leaves price unchanged when net transfers are below threshold", () => {
      const result = calculatePriceAdjustment(8.0, 3, { threshold: 5 });
      assert.equal(result.newPrice, 8.0);
      assert.equal(result.diff, 0.0);
      assert.equal(result.adjusted, false);
      assert.ok(result.reason.includes("price unchanged"));
    });

    it("caps price increase at max ±£0.1m even with massive net transfer demand", () => {
      const result = calculatePriceAdjustment(9.5, 50_000, { threshold: 5 });
      assert.equal(result.newPrice, 9.6);
      assert.equal(result.diff, MAX_PRICE_CHANGE_PER_24H);
    });

    it("caps price decrease at max ±£0.1m even with massive net selling", () => {
      const result = calculatePriceAdjustment(9.5, -50_000, { threshold: 5 });
      assert.equal(result.newPrice, 9.4);
      assert.equal(result.diff, -MAX_PRICE_CHANGE_PER_24H);
    });

    it("strictly prevents price from dropping below £3.5m floor", () => {
      const result = calculatePriceAdjustment(MIN_PLAYER_PRICE, -100, { threshold: 5 });
      assert.equal(result.newPrice, MIN_PLAYER_PRICE);
      assert.equal(result.diff, 0);
      assert.equal(result.adjusted, false);
      assert.ok(result.reason.includes("Clamped to minimum boundary"));
    });

    it("strictly prevents price from exceeding £15.0m ceiling", () => {
      const result = calculatePriceAdjustment(MAX_PLAYER_PRICE, 100, { threshold: 5 });
      assert.equal(result.newPrice, MAX_PLAYER_PRICE);
      assert.equal(result.diff, 0);
      assert.equal(result.adjusted, false);
      assert.ok(result.reason.includes("Clamped to maximum boundary"));
    });
  });

  describe("PlayerPricingEngine Lifecycle & Idempotency", () => {
    function createMockPrisma() {
      const players = [
        { id: 1, fplId: 101, price: 8.0 },
        { id: 2, fplId: 102, price: 3.5 },
        { id: 3, fplId: 103, price: 15.0 },
      ];

      const priceChanges: Array<{
        playerId: number;
        fplId: number;
        oldPrice: any;
        newPrice: any;
        diff: any;
        changedAt: Date;
      }> = [];

      // Transfers: Player 1 has 10 ins, 2 outs (net +8)
      // Player 2 has 1 in, 20 outs (net -19)
      const transfers = [
        ...Array(10).fill({ playerInId: 1, playerOutId: 99, timestamp: new Date() }),
        ...Array(2).fill({ playerInId: 99, playerOutId: 1, timestamp: new Date() }),
        ...Array(1).fill({ playerInId: 2, playerOutId: 99, timestamp: new Date() }),
        ...Array(20).fill({ playerInId: 99, playerOutId: 2, timestamp: new Date() }),
      ];

      return {
        player: {
          findMany: async () => players,
          findUnique: async ({ where }: any) => players.find((p) => p.id === where.id) ?? null,
          update: async ({ where, data }: any) => {
            const player = players.find((p) => p.id === where.id);
            if (player) {
              player.price = parseFloat(data.price);
            }
            return player;
          },
        },
        squadTransfer: {
          count: async ({ where }: any) => {
            if (where.playerInId) {
              return transfers.filter((t) => t.playerInId === where.playerInId).length;
            }
            if (where.playerOutId) {
              return transfers.filter((t) => t.playerOutId === where.playerOutId).length;
            }
            return 0;
          },
        },
        playerPriceChange: {
          create: async ({ data }: any) => {
            priceChanges.push({ ...data });
            return data;
          },
          findFirst: async ({ where }: any) => {
            return (
              priceChanges
                .filter((pc) => pc.playerId === where.playerId && pc.changedAt >= where.changedAt.gte)
                .slice(-1)[0] ?? null
            );
          },
        },
        _state: { players, priceChanges },
      };
    }

    it("calculates net transfer volume correctly over the rolling window", async () => {
      const mockPrisma = createMockPrisma();
      const engine = new PlayerPricingEngine(mockPrisma as unknown as PrismaClient);

      const netStats = await engine.calculateNetTransfers(1);
      assert.equal(netStats.transfersIn, 10);
      assert.equal(netStats.transfersOut, 2);
      assert.equal(netStats.netTransfers, 8);
    });

    it("updates player price and records audit history in playerPriceChange", async () => {
      const mockPrisma = createMockPrisma();
      const engine = new PlayerPricingEngine(mockPrisma as unknown as PrismaClient);

      const result = await engine.recalculatePlayerPrice(1, { threshold: 5 });
      assert.ok(result);
      assert.equal(result.oldPrice, 8.0);
      assert.equal(result.newPrice, 8.1);
      assert.equal(result.diff, 0.1);

      // Verify DB was updated
      const updatedPlayer = await mockPrisma.player.findUnique({ where: { id: 1 } });
      assert.equal(updatedPlayer!.price, 8.1);

      // Verify audit history record
      assert.equal(mockPrisma._state.priceChanges.length, 1);
      assert.equal(mockPrisma._state.priceChanges[0].playerId, 1);
      assert.equal(mockPrisma._state.priceChanges[0].diff, "0.1");
    });

    it("enforces idempotency by skipping players already adjusted within the 24h rolling window", async () => {
      const mockPrisma = createMockPrisma();
      const engine = new PlayerPricingEngine(mockPrisma as unknown as PrismaClient);

      // First run: adjusts price from 8.0 to 8.1
      const firstRun = await engine.recalculatePlayerPrice(1, { threshold: 5 });
      assert.ok(firstRun);
      assert.equal(firstRun.newPrice, 8.1);
      assert.equal(mockPrisma._state.priceChanges.length, 1);

      // Immediate second run (simulating duplicate daily cron trigger)
      const secondRun = await engine.recalculatePlayerPrice(1, { threshold: 5 });
      // Should skip and return null without adjusting price again
      assert.equal(secondRun, null);
      assert.equal(mockPrisma._state.priceChanges.length, 1);

      const player = await mockPrisma.player.findUnique({ where: { id: 1 } });
      assert.equal(player!.price, 8.1); // Did not double-adjust to 8.2
    });

    it("evaluates all players and respects boundary caps in full batch recalculation", async () => {
      const mockPrisma = createMockPrisma();
      const engine = new PlayerPricingEngine(mockPrisma as unknown as PrismaClient);

      const summary = await engine.recalculateAllPlayerPrices({ threshold: 5 });
      assert.equal(summary.totalEvaluated, 3);
      // Player 1 adjusted (+0.1)
      // Player 2 attempted -0.1 but clamped at floor 3.5 (unchanged)
      // Player 3 has no transfers (unchanged)
      assert.equal(summary.adjustedCount, 1);

      const player2 = await mockPrisma.player.findUnique({ where: { id: 2 } });
      assert.equal(player2!.price, 3.5); // Remains at floor
    });
  });
});
