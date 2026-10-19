/**
 * External FPL API Client with Redis & In-Memory Caching Layer and Circuit Breaker.
 *
 * Provides typed methods to query the official Fantasy Premier League endpoints:
 * - /bootstrap-static/ (teams, players, gameweeks)
 * - /fixtures/ (match fixtures)
 * - /event/{gw}/live/ (live player stats per gameweek)
 *
 * Implements a Redis caching layer (with in-memory fallback) and a robust Circuit Breaker
 * with adaptive backoff (decorrelated jitter) and stale cache fallback.
 */

import { getRedisClient, IRedisCacheClient } from "../../config/redis.js";
import {
  CircuitBreaker,
  CircuitBreakerOptions,
  CircuitBreakerOpenError,
  calculateDecorrelatedJitter,
} from "./circuitBreaker.js";

const FPL_BASE_URL = "https://fantasy.premierleague.com/api";

/**
 * Cache TTLs for FPL endpoints (issue #139).
 */
export const FPL_CACHE_TTL = {
  DEFAULT_SECONDS: 600, // 10 minutes
  BOOTSTRAP_STATIC_SECONDS: 24 * 60 * 60, // 24 hours: static player/team/gameweek data
  FIXTURES_SECONDS: 24 * 60 * 60, // 24 hours: fixtures rarely change once scheduled
  GAMEWEEK_LIVE_SECONDS: 60, // 60 seconds: live in-match scores
};

interface CacheEntry<T> {
  data: T;
  expiresAt: number;
}

export interface FplClientOptions {
  baseUrl?: string;
  redis?: IRedisCacheClient | null;
  keyPrefix?: string;
  defaultTtlSeconds?: number;
  circuitBreaker?: CircuitBreaker;
  circuitBreakerOptions?: CircuitBreakerOptions;
}

export class FplClient {
  private cache = new Map<string, CacheEntry<unknown>>();
  private staleCache = new Map<string, unknown>();
  private baseUrl: string;
  private redis: IRedisCacheClient | null = null;
  private keyPrefix: string;
  private defaultTtlSeconds: number;

  public readonly circuitBreaker: CircuitBreaker;

  constructor(
    baseUrlOrOptions?: string | FplClientOptions,
    redisClient?: IRedisCacheClient | null
  ) {
    if (typeof baseUrlOrOptions === "object" && baseUrlOrOptions !== null) {
      this.baseUrl = baseUrlOrOptions.baseUrl || FPL_BASE_URL;
      this.redis =
        baseUrlOrOptions.redis !== undefined
          ? baseUrlOrOptions.redis
          : getRedisClient();
      this.keyPrefix = baseUrlOrOptions.keyPrefix || "fpl:cache:";
      this.defaultTtlSeconds =
        baseUrlOrOptions.defaultTtlSeconds || FPL_CACHE_TTL.DEFAULT_SECONDS;
      this.circuitBreaker =
        baseUrlOrOptions.circuitBreaker ||
        new CircuitBreaker(baseUrlOrOptions.circuitBreakerOptions);
    } else {
      this.baseUrl = baseUrlOrOptions || FPL_BASE_URL;
      this.redis = redisClient !== undefined ? redisClient : getRedisClient();
      this.keyPrefix = "fpl:cache:";
      this.defaultTtlSeconds = FPL_CACHE_TTL.DEFAULT_SECONDS;
      this.circuitBreaker = new CircuitBreaker();
    }
  }

  /**
   * Allows injecting or replacing the Redis cache client (e.g. for testing).
   */
  public setRedisClient(client: IRedisCacheClient | null): void {
    this.redis = client;
  }

  /**
   * Returns current Redis cache client or null.
   */
  public getRedisClient(): IRedisCacheClient | null {
    return this.redis;
  }

  private getCacheKey(endpoint: string): string {
    return `${this.keyPrefix}${endpoint}`;
  }

  /**
   * Fetches an upstream URL with retry logic and decorrelated jitter backoff.
   * Throws on 5xx or 429 errors so the circuit breaker can track failures.
   */
  public async fetchWithRetry(url: string, retries = 3, backoffMs = 1000): Promise<Response> {
    let currentDelay = backoffMs;

    for (let i = 0; i < retries; i++) {
      try {
        const response = await fetch(url, {
          headers: {
            "User-Agent": "FantasyXI-App/1.0 (Educational open-source fantasy football project)",
            Accept: "application/json",
          },
          signal: AbortSignal.timeout(10000),
        });

        if (response.ok) {
          return response;
        }

        // Retry on rate limit (429) or server errors (5xx)
        if (response.status === 429 || response.status >= 500) {
          throw new Error(`FPL API failed: [${response.status}] ${response.statusText}`);
        }

        // Do not retry on client errors (400-404)
        return response;
      } catch (err) {
        if (i === retries - 1) {
          throw err;
        }
        currentDelay = calculateDecorrelatedJitter(currentDelay, backoffMs, 30000);
        console.warn(
          `⚠️ FPL API request failed. Retrying in ${currentDelay}ms (decorrelated jitter)... (Attempt ${i + 1}/${retries})`
        );
        await new Promise((resolve) => setTimeout(resolve, currentDelay));
      }
    }
    throw new Error("Unreachable");
  }

  /**
   * Generic fetch wrapper with Redis caching, in-memory fallback, Circuit Breaker,
   * and fallback to stale data if upstream is failing or rate-limited.
   */
  public async get<T>(endpoint: string, ttlSeconds: number = this.defaultTtlSeconds): Promise<T> {
    const { data } = await this.getWithMetadata<T>(endpoint, ttlSeconds);
    return data;
  }

  /**
   * Fetch with metadata indicating whether the data returned is fresh or served stale.
   */
  public async getWithMetadata<T>(
    endpoint: string,
    ttlSeconds: number = this.defaultTtlSeconds
  ): Promise<{ data: T; isStale: boolean }> {
    const effectiveTtlSeconds = ttlSeconds;
    const cacheKey = this.getCacheKey(endpoint);

    // 1. Try Redis cache first
    if (this.redis) {
      try {
        const cachedRaw = await this.redis.get(cacheKey);
        if (cachedRaw) {
          console.log(`[FplClient] Cache HIT (Redis) for ${cacheKey}`);
          const parsed = JSON.parse(cachedRaw) as T;
          this.cache.set(cacheKey, {
            data: parsed,
            expiresAt: Date.now() + effectiveTtlSeconds * 1000,
          });
          this.staleCache.set(cacheKey, parsed);
          return { data: parsed, isStale: false };
        }
      } catch (err) {
        console.warn(
          `[FplClient] Redis GET error for ${cacheKey}, falling back:`,
          (err as Error).message
        );
      }
    }

    // 2. Try in-memory fallback cache
    const memoryCached = this.cache.get(cacheKey);
    if (memoryCached && Date.now() < memoryCached.expiresAt) {
      console.log(`[FplClient] Cache HIT (Memory) for ${cacheKey}`);
      this.staleCache.set(cacheKey, memoryCached.data);
      return { data: memoryCached.data as T, isStale: false };
    }

    // 3. Cache MISS: Fetch from upstream official FPL API via Circuit Breaker
    console.log(`[FplClient] Cache MISS for ${cacheKey}. Fetching upstream from ${this.baseUrl}...`);
    const url = `${this.baseUrl}${endpoint}`;

    try {
      const response = await this.circuitBreaker.execute(async () => {
        return await this.fetchWithRetry(url);
      });

      if (!response.ok) {
        throw new Error(
          `FPL API request failed permanently: [${response.status}] ${response.statusText} at ${url}`
        );
      }

      const data = (await response.json()) as T;

      // 4. Populate Redis cache
      if (this.redis) {
        try {
          await this.redis.set(cacheKey, JSON.stringify(data), "EX", effectiveTtlSeconds);
          console.log(
            `[FplClient] Cache SET (Redis) for ${cacheKey} (TTL: ${effectiveTtlSeconds}s)`
          );
        } catch (err) {
          console.warn(
            `[FplClient] Redis SET error for ${cacheKey}:`,
            (err as Error).message
          );
        }
      }

      // 5. Populate in-memory caches
      this.cache.set(cacheKey, {
        data,
        expiresAt: Date.now() + effectiveTtlSeconds * 1000,
      });
      this.staleCache.set(cacheKey, data);

      return { data, isStale: false };
    } catch (err) {
      // 6. Upstream or Circuit Breaker failed: Fallback to STALE cache if available
      if (this.staleCache.has(cacheKey)) {
        console.warn(
          `[FplClient] Upstream unavailable (${(err as Error).message}). Serving STALE cached data for ${cacheKey}`
        );
        return { data: this.staleCache.get(cacheKey) as T, isStale: true };
      }

      if (memoryCached) {
        console.warn(
          `[FplClient] Upstream unavailable (${(err as Error).message}). Serving expired in-memory STALE data for ${cacheKey}`
        );
        return { data: memoryCached.data as T, isStale: true };
      }

      throw err;
    }
  }

  /**
   * Clear cache manually (e.g. before an explicit force-sync).
   * Clears both in-memory cache and Redis keys.
   */
  public async clearCache(endpoint?: string): Promise<void> {
    if (endpoint) {
      const cacheKey = this.getCacheKey(endpoint);
      this.cache.delete(cacheKey);
      this.staleCache.delete(cacheKey);
      if (this.redis) {
        try {
          await this.redis.del(cacheKey);
          console.log(`[FplClient] Cache DEL (Redis) for ${cacheKey}`);
        } catch (err) {
          console.warn(`[FplClient] Redis DEL error for ${cacheKey}:`, (err as Error).message);
        }
      }
    } else {
      this.cache.clear();
      this.staleCache.clear();
      if (this.redis) {
        try {
          const keys = await this.redis.keys(`${this.keyPrefix}*`);
          if (keys.length > 0) {
            await this.redis.del(...keys);
            console.log(`[FplClient] Cache DEL (Redis) cleared ${keys.length} keys`);
          }
        } catch (err) {
          console.warn(`[FplClient] Redis DEL pattern error:`, (err as Error).message);
        }
      }
    }
  }

  /**
   * Fetches the core FPL dataset:
   * - events (all 38 gameweeks with deadlines)
   * - teams (all 20 Premier League clubs)
   * - elements (all ~600 players with current prices, totals, form)
   */
  public async getBootstrapStatic(): Promise<{
    events: Array<{
      id: number;
      name: string;
      deadline_time: string;
      is_previous: boolean;
      is_current: boolean;
      is_next: boolean;
      finished: boolean;
    }>;
    teams: Array<{
      id: number;
      name: string;
      short_name: string;
      strength?: number;
      strength_overall_home?: number;
      strength_overall_away?: number;
      strength_attack_home?: number;
      strength_attack_away?: number;
      strength_defence_home?: number;
      strength_defence_away?: number;
    }>;
    elements: Array<{
      id: number;
      first_name: string;
      second_name: string;
      web_name: string;
      element_type: number; // 1 = GKP, 2 = DEF, 3 = MID, 4 = FWD
      team: number;
      now_cost: number; // Tenths of a million: e.g. 105 = 10.5m
      total_points: number;
      minutes: number;
      goals_scored: number;
      assists: number;
      clean_sheets: number;
      form?: string;
      status?: string;
      news?: string;
      chance_of_playing_next_round?: number | null;
      selected_by_percent?: string;
      photo?: string;
    }>;
  }> {
    return this.get("/bootstrap-static/", FPL_CACHE_TTL.BOOTSTRAP_STATIC_SECONDS);
  }

  /**
   * Fetches fixtures for the entire season or filtered by gameweek event ID.
   */
  public async getFixtures(eventId?: number): Promise<
    Array<{
      id: number;
      event: number | null;
      team_h: number;
      team_a: number;
      team_h_score: number | null;
      team_a_score: number | null;
      kickoff_time: string | null;
      started: boolean;
      finished: boolean;
      finished_provisional?: boolean;
      minutes: number;
      stats?: Array<{
        identifier: string;
        h: Array<{ value: number; element: number }>;
        a: Array<{ value: number; element: number }>;
      }>;
    }>
  > {
    const endpoint = eventId !== undefined ? `/fixtures/?event=${eventId}` : "/fixtures/";
    return this.get(endpoint, FPL_CACHE_TTL.FIXTURES_SECONDS);
  }

  /**
   * Fetches live in-match stats for all players in a specific gameweek.
   */
  public async getGameweekLive(gameweekId: number): Promise<{
    elements: Array<{
      id: number;
      stats: {
        minutes: number;
        goals_scored: number;
        assists: number;
        clean_sheets: number;
        goals_conceded: number;
        own_goals: number;
        penalties_saved?: number;
        penalties_missed?: number;
        yellow_cards: number;
        red_cards: number;
        saves: number;
        bonus: number;
        total_points: number;
      };
      explain: Array<{
        fixture: number;
        stats: Array<{
          identifier: string;
          points: number;
          value: number;
        }>;
      }>;
    }>;
  }> {
    return this.get(`/event/${gameweekId}/live/`, FPL_CACHE_TTL.GAMEWEEK_LIVE_SECONDS);
  }
}

export const fplClient = new FplClient();
