import type { PrismaClient } from "@prisma/client";
import { prisma } from "../../config/db.js";

export const MIN_PLAYER_PRICE = 3.5;
export const MAX_PLAYER_PRICE = 15.0;
export const MAX_PRICE_CHANGE_PER_24H = 0.1;
export const DEFAULT_NET_TRANSFER_THRESHOLD = 5;
export const ROLLING_WINDOW_MS = 24 * 60 * 60 * 1000;

export interface PriceAdjustmentResult {
  newPrice: number;
  diff: number;
  adjusted: boolean;
  reason: string;
}

export interface PlayerPriceAdjustmentResult {
  playerId: number;
  fplId: number;
  oldPrice: number;
  newPrice: number;
  diff: number;
  netTransfers: number;
  reason: string;
  adjustedAt: string;
}

export interface RecalculationSummary {
  totalEvaluated: number;
  adjustedCount: number;
  skippedCount: number;
  unchangedCount: number;
  results: PlayerPriceAdjustmentResult[];
}

export interface RecalculateOptions {
  threshold?: number;
  windowMs?: number;
  referenceDate?: Date;
  force?: boolean;
}

/**
 * Calculates a player's new price based on net transfer volume and boundary constraints.
 */
export function calculatePriceAdjustment(
  currentPrice: number,
  netTransfers: number,
  options: {
    threshold?: number;
    minPrice?: number;
    maxPrice?: number;
    maxChange?: number;
  } = {}
): PriceAdjustmentResult {
  const threshold = options.threshold ?? DEFAULT_NET_TRANSFER_THRESHOLD;
  const minPrice = options.minPrice ?? MIN_PLAYER_PRICE;
  const maxPrice = options.maxPrice ?? MAX_PLAYER_PRICE;
  const maxChange = options.maxChange ?? MAX_PRICE_CHANGE_PER_24H;

  let rawDelta = 0;
  let reason = "Net transfers within threshold, price unchanged";

  if (netTransfers >= threshold) {
    rawDelta = maxChange;
    reason = `Heavy net transfers in (+${netTransfers}) reached threshold (+${threshold})`;
  } else if (netTransfers <= -threshold) {
    rawDelta = -maxChange;
    reason = `Heavy net transfers out (${netTransfers}) reached sell threshold (-${threshold})`;
  }

  const roundedCurrent = Math.round(currentPrice * 10) / 10;
  const targetPrice = Math.round((roundedCurrent + rawDelta) * 10) / 10;
  const clampedPrice = Math.max(minPrice, Math.min(maxPrice, targetPrice));
  const diff = Math.round((clampedPrice - roundedCurrent) * 10) / 10;
  const adjusted = diff !== 0;

  if (targetPrice < minPrice && rawDelta < 0) {
    reason += ` (Clamped to minimum boundary £${minPrice.toFixed(1)}m)`;
  } else if (targetPrice > maxPrice && rawDelta > 0) {
    reason += ` (Clamped to maximum boundary £${maxPrice.toFixed(1)}m)`;
  }

  return {
    newPrice: clampedPrice,
    diff,
    adjusted,
    reason,
  };
}

/**
 * Player Pricing Engine.
 *
 * Evaluates internal transfer velocity (transfers in vs transfers out) over a rolling 24-hour window
 * and adjusts player market prices by up to ±£0.1m within absolute boundaries (£3.5m - £15.0m).
 * Ensures idempotent execution so daily cron jobs will not double-apply price adjustments.
 */
export class PlayerPricingEngine {
  constructor(private readonly db: PrismaClient = prisma) {}

  /**
   * Calculates net transfers for a player across the rolling window.
   */
  public async calculateNetTransfers(
    playerId: number,
    windowMs: number = ROLLING_WINDOW_MS,
    referenceDate: Date = new Date()
  ): Promise<{ transfersIn: number; transfersOut: number; netTransfers: number }> {
    const windowStart = new Date(referenceDate.getTime() - windowMs);

    const [transfersIn, transfersOut] = await Promise.all([
      this.db.squadTransfer.count({
        where: {
          playerInId: playerId,
          timestamp: { gte: windowStart, lte: referenceDate },
        },
      }),
      this.db.squadTransfer.count({
        where: {
          playerOutId: playerId,
          timestamp: { gte: windowStart, lte: referenceDate },
        },
      }),
    ]);

    return {
      transfersIn,
      transfersOut,
      netTransfers: transfersIn - transfersOut,
    };
  }

  /**
   * Recalculates and adjusts a single player's price if criteria are met.
   * Enforces idempotency by skipping players whose prices were already changed within the current window.
   */
  public async recalculatePlayerPrice(
    playerId: number,
    options: RecalculateOptions = {}
  ): Promise<PlayerPriceAdjustmentResult | null> {
    const now = options.referenceDate ?? new Date();
    const windowMs = options.windowMs ?? ROLLING_WINDOW_MS;
    const windowStart = new Date(now.getTime() - windowMs);

    const player = await this.db.player.findUnique({
      where: { id: playerId },
      select: { id: true, fplId: true, price: true },
    });

    if (!player) {
      return null;
    }

    // Idempotency: skip if price was already adjusted within the 24h rolling window
    if (!options.force) {
      const recentChange = await this.db.playerPriceChange.findFirst({
        where: {
          playerId: player.id,
          changedAt: { gte: windowStart },
        },
        orderBy: { changedAt: "desc" },
      });

      if (recentChange) {
        return null;
      }
    }

    const { netTransfers } = await this.calculateNetTransfers(player.id, windowMs, now);
    const currentPriceNum = Number(player.price);
    const { newPrice, diff, adjusted, reason } = calculatePriceAdjustment(
      currentPriceNum,
      netTransfers,
      { threshold: options.threshold }
    );

    if (!adjusted) {
      return {
        playerId: player.id,
        fplId: player.fplId,
        oldPrice: currentPriceNum,
        newPrice: currentPriceNum,
        diff: 0,
        netTransfers,
        reason,
        adjustedAt: now.toISOString(),
      };
    }

    // Persist price change audit record & update player record
    await this.db.playerPriceChange.create({
      data: {
        playerId: player.id,
        fplId: player.fplId,
        oldPrice: player.price,
        newPrice: newPrice.toFixed(1) as any,
        diff: diff.toFixed(1) as any,
        changedAt: now,
      },
    });

    await this.db.player.update({
      where: { id: player.id },
      data: {
        price: newPrice.toFixed(1) as any,
      },
    });

    return {
      playerId: player.id,
      fplId: player.fplId,
      oldPrice: currentPriceNum,
      newPrice,
      diff,
      netTransfers,
      reason,
      adjustedAt: now.toISOString(),
    };
  }

  /**
   * Evaluates all players and adjusts eligible prices. Suitable for daily cron execution.
   */
  public async recalculateAllPlayerPrices(
    options: RecalculateOptions = {}
  ): Promise<RecalculationSummary> {
    const players = await this.db.player.findMany({
      select: { id: true },
    });

    const summary: RecalculationSummary = {
      totalEvaluated: players.length,
      adjustedCount: 0,
      skippedCount: 0,
      unchangedCount: 0,
      results: [],
    };

    for (const player of players) {
      const result = await this.recalculatePlayerPrice(player.id, options);
      if (!result) {
        summary.skippedCount++;
        continue;
      }

      if (result.diff !== 0) {
        summary.adjustedCount++;
      } else {
        summary.unchangedCount++;
      }
      summary.results.push(result);
    }

    return summary;
  }
}

export const playerPricingEngine = new PlayerPricingEngine();
