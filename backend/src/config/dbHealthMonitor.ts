/**
 * Multi-Region Database Cluster Health Monitor & Primary Election Service (Issue #218).
 *
 * Provides:
 * - 5-second health-check polling of primary database and read replicas
 * - Replication lag tracking: isolates replicas exceeding lag threshold (> 1000ms)
 * - Automatic read-only maintenance mode engagement when primary fails
 * - Standardized 503 Service Unavailable responses with Retry-After: 30 for write mutations during failover
 * - Automatic primary candidate election based on lowest replication lag and latency
 * - Seamless recovery when primary connection restores
 */

import { Request, Response, NextFunction } from "express";

export const DEFAULT_CHECK_INTERVAL_MS = 5_000;
export const DEFAULT_MAX_LAG_THRESHOLD_MS = 1_000;
export const DEFAULT_HEALTH_TIMEOUT_MS = 2_000;

export interface DatabaseNodeClient {
  $queryRawUnsafe<T = unknown>(query: string, ...values: unknown[]): Promise<T>;
  $disconnect?(): Promise<void>;
  [key: string]: any;
}

export interface NodeStatus {
  region: string;
  healthy: boolean;
  latencyMs: number | null;
  lastCheckedAt: Date | null;
  lastError: string | null;
}

export interface ReplicaNodeStatus extends NodeStatus {
  lagMs: number | null;
  isolated: boolean;
  client: DatabaseNodeClient;
}

export interface PrimaryNodeStatus extends NodeStatus {
  client: DatabaseNodeClient;
}

export interface ClusterMonitorOptions {
  checkIntervalMs?: number;
  maxLagMs?: number;
  healthTimeoutMs?: number;
  primary: { client: DatabaseNodeClient; region?: string };
  replicas?: Array<{ region: string; client: DatabaseNodeClient }>;
  onPrimaryFailure?: () => void;
  onPrimaryRecovery?: () => void;
  onCandidateElected?: (candidateRegion: string) => void;
}

export interface ClusterHealthStatus {
  isMaintenanceMode: boolean;
  isReadOnly: boolean;
  primary: {
    region: string;
    healthy: boolean;
    latencyMs: number | null;
    lastError: string | null;
  };
  replicas: Array<{
    region: string;
    healthy: boolean;
    isolated: boolean;
    lagMs: number | null;
    latencyMs: number | null;
    lastError: string | null;
  }>;
  electedCandidate: string | null;
}

export const REPLICATION_LAG_SQL = `
  SELECT CASE
    WHEN NOT pg_is_in_recovery() THEN 0
    WHEN pg_last_wal_receive_lsn() = pg_last_wal_replay_lsn() THEN 0
    ELSE COALESCE(EXTRACT(EPOCH FROM (now() - pg_last_xact_replay_timestamp())) * 1000, 0)
  END::float8 AS lag_ms
`;

export class DatabaseClusterMonitor {
  public readonly checkIntervalMs: number;
  public readonly maxLagMs: number;
  public readonly healthTimeoutMs: number;

  private isMaintenanceMode = false;
  private primary: PrimaryNodeStatus;
  private replicas: Map<string, ReplicaNodeStatus> = new Map();
  private electedCandidate: string | null = null;
  private timer: NodeJS.Timeout | null = null;

  private onPrimaryFailure?: () => void;
  private onPrimaryRecovery?: () => void;
  private onCandidateElected?: (candidateRegion: string) => void;

  constructor(options: ClusterMonitorOptions) {
    this.checkIntervalMs = options.checkIntervalMs ?? DEFAULT_CHECK_INTERVAL_MS;
    this.maxLagMs = options.maxLagMs ?? DEFAULT_MAX_LAG_THRESHOLD_MS;
    this.healthTimeoutMs = options.healthTimeoutMs ?? DEFAULT_HEALTH_TIMEOUT_MS;
    this.onPrimaryFailure = options.onPrimaryFailure;
    this.onPrimaryRecovery = options.onPrimaryRecovery;
    this.onCandidateElected = options.onCandidateElected;

    this.primary = {
      region: options.primary.region ?? "primary",
      client: options.primary.client,
      healthy: true,
      latencyMs: null,
      lastCheckedAt: null,
      lastError: null,
    };

    if (options.replicas) {
      for (const r of options.replicas) {
        this.replicas.set(r.region, {
          region: r.region,
          client: r.client,
          healthy: false,
          isolated: false,
          lagMs: null,
          latencyMs: null,
          lastCheckedAt: null,
          lastError: null,
        });
      }
    }
  }

  public getIsMaintenanceMode(): boolean {
    return this.isMaintenanceMode;
  }

  public isReadOnly(): boolean {
    return this.isMaintenanceMode;
  }

  public getElectedCandidate(): string | null {
    return this.electedCandidate;
  }

  /**
   * Health checks the primary database.
   * If primary fails, engages read-only maintenance mode and triggers candidate election.
   * If primary restores, disengages maintenance mode.
   */
  public async checkPrimary(): Promise<boolean> {
    const startedAt = performance.now();
    let timeoutId: NodeJS.Timeout | undefined;

    try {
      await Promise.race([
        this.primary.client.$queryRawUnsafe("SELECT 1"),
        new Promise<never>((_, reject) => {
          timeoutId = setTimeout(
            () => reject(new Error(`Primary health check timed out after ${this.healthTimeoutMs}ms`)),
            this.healthTimeoutMs
          );
        }),
      ]);

      const latency = Math.round((performance.now() - startedAt) * 100) / 100;
      this.primary.healthy = true;
      this.primary.latencyMs = latency;
      this.primary.lastCheckedAt = new Date();
      this.primary.lastError = null;

      if (this.isMaintenanceMode) {
        console.log("✅ Primary database connection restored. Disengaging read-only maintenance mode.");
        this.isMaintenanceMode = false;
        this.electedCandidate = null;
        if (this.onPrimaryRecovery) {
          this.onPrimaryRecovery();
        }
      }

      return true;
    } catch (err) {
      const errorMsg = (err as Error)?.message ?? String(err);
      this.primary.healthy = false;
      this.primary.lastCheckedAt = new Date();
      this.primary.lastError = errorMsg;

      if (!this.isMaintenanceMode) {
        console.warn(`🚨 Primary database connection failure: ${errorMsg}. Engaging READ-ONLY maintenance mode.`);
        this.isMaintenanceMode = true;
        this.electCandidate();
        if (this.onPrimaryFailure) {
          this.onPrimaryFailure();
        }
      }

      return false;
    } finally {
      if (timeoutId) clearTimeout(timeoutId);
    }
  }

  /**
   * Checks replication lag on all replicas and isolates any that exceed maxLagMs.
   */
  public async checkReplicas(): Promise<void> {
    const checkPromises = Array.from(this.replicas.values()).map(async (node) => {
      const startedAt = performance.now();
      let timeoutId: NodeJS.Timeout | undefined;

      try {
        const rows = await Promise.race([
          node.client.$queryRawUnsafe<Array<{ lag_ms: number | string }>>(REPLICATION_LAG_SQL),
          new Promise<never>((_, reject) => {
            timeoutId = setTimeout(
              () => reject(new Error(`Replica ${node.region} health check timed out`)),
              this.healthTimeoutMs
            );
          }),
        ]);

        const lagMs = Number(rows?.[0]?.lag_ms);
        if (!Number.isFinite(lagMs)) {
          throw new Error("Replica returned non-finite replication lag");
        }

        const latency = Math.round((performance.now() - startedAt) * 100) / 100;
        node.latencyMs = latency;
        node.lagMs = Math.round(lagMs);
        node.lastCheckedAt = new Date();
        node.lastError = null;

        // Threshold check: isolate replica if replication lag exceeds limit
        if (node.lagMs > this.maxLagMs) {
          node.isolated = true;
          node.healthy = false;
          console.warn(
            `⚠️ Read replica ${node.region} ISOLATED: replication lag (${node.lagMs}ms) exceeds threshold (${this.maxLagMs}ms)`
          );
        } else {
          node.isolated = false;
          node.healthy = true;
        }
      } catch (err) {
        node.healthy = false;
        node.isolated = true;
        node.lastCheckedAt = new Date();
        node.lastError = (err as Error)?.message ?? String(err);
        console.warn(`⚠️ Read replica ${node.region} unhealthy/isolated: ${node.lastError}`);
      } finally {
        if (timeoutId) clearTimeout(timeoutId);
      }
    });

    await Promise.all(checkPromises);

    // If primary is down, recalculate candidate
    if (this.isMaintenanceMode) {
      this.electCandidate();
    }
  }

  /**
   * Evaluates healthy, non-isolated replicas to elect the best promotion candidate.
   * Priority: Lowest replication lag, followed by lowest network latency.
   */
  public electCandidate(): string | null {
    const eligible = Array.from(this.replicas.values()).filter(
      (node) => node.healthy && !node.isolated && node.lagMs !== null
    );

    if (eligible.length === 0) {
      this.electedCandidate = null;
      return null;
    }

    eligible.sort((a, b) => {
      const lagDiff = (a.lagMs ?? Infinity) - (b.lagMs ?? Infinity);
      if (lagDiff !== 0) return lagDiff;
      return (a.latencyMs ?? Infinity) - (b.latencyMs ?? Infinity);
    });

    const chosen = eligible[0];
    if (this.electedCandidate !== chosen.region) {
      this.electedCandidate = chosen.region;
      console.log(
        `👑 Elected primary failover candidate: ${chosen.region} (lag: ${chosen.lagMs}ms, latency: ${chosen.latencyMs}ms)`
      );
      if (this.onCandidateElected) {
        this.onCandidateElected(chosen.region);
      }
    }

    return this.electedCandidate;
  }

  /**
   * Performs an immediate full health check pass on primary and all replicas.
   */
  public async checkAll(): Promise<ClusterHealthStatus> {
    await Promise.all([this.checkPrimary(), this.checkReplicas()]);
    return this.getClusterStatus();
  }

  public getEligibleReplicas(): ReplicaNodeStatus[] {
    return Array.from(this.replicas.values()).filter((n) => n.healthy && !n.isolated);
  }

  public getClusterStatus(): ClusterHealthStatus {
    return {
      isMaintenanceMode: this.isMaintenanceMode,
      isReadOnly: this.isMaintenanceMode,
      primary: {
        region: this.primary.region,
        healthy: this.primary.healthy,
        latencyMs: this.primary.latencyMs,
        lastError: this.primary.lastError,
      },
      replicas: Array.from(this.replicas.values()).map((r) => ({
        region: r.region,
        healthy: r.healthy,
        isolated: r.isolated,
        lagMs: r.lagMs,
        latencyMs: r.latencyMs,
        lastError: r.lastError,
      })),
      electedCandidate: this.electedCandidate,
    };
  }

  public start(): void {
    if (this.timer) return;
    this.timer = setInterval(() => {
      this.checkAll().catch((err) => {
        console.error("[DatabaseClusterMonitor] Unhandled error during periodic health check:", err);
      });
    }, this.checkIntervalMs);
  }

  public stop(): void {
    if (this.timer) {
      clearInterval(this.timer);
      this.timer = null;
    }
  }
}

/**
 * Express middleware that checks database cluster health.
 * When primary is unavailable and maintenance mode is engaged:
 * - Read requests (GET, HEAD, OPTIONS) proceed seamlessly to read replicas
 * - Write requests (POST, PUT, PATCH, DELETE) receive HTTP 503 Service Unavailable with Retry-After: 30
 */
export function createDatabaseMaintenanceMiddleware(monitor: DatabaseClusterMonitor) {
  return function databaseMaintenanceMiddleware(req: Request, res: Response, next: NextFunction): void {
    if (monitor.getIsMaintenanceMode()) {
      const isReadMethod = req.method === "GET" || req.method === "HEAD" || req.method === "OPTIONS";

      if (!isReadMethod) {
        res.setHeader("Retry-After", "30");
        res.status(503).json({
          error: "Database maintenance in progress: primary database is temporarily unavailable. Read-only mode engaged.",
          readOnly: true,
          retryAfter: 30,
          electedCandidate: monitor.getElectedCandidate(),
        });
        return;
      }
    }
    next();
  };
}
