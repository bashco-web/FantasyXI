import { describe, it, beforeEach, afterEach } from "node:test";
import assert from "node:assert/strict";
import { Request, Response } from "express";
import {
  DatabaseClusterMonitor,
  createDatabaseMaintenanceMiddleware,
  DatabaseNodeClient,
} from "../config/dbHealthMonitor.js";

function mockDbNode(options: {
  lagMs?: number;
  fail?: Error | null;
  delayMs?: number;
} = {}) {
  let fail = options.fail ?? null;
  let lagMs = options.lagMs ?? 0;

  const client: DatabaseNodeClient = {
    $queryRawUnsafe: async <T>(query: string): Promise<T> => {
      if (options.delayMs) {
        await new Promise((r) => setTimeout(r, options.delayMs));
      }
      if (fail) throw fail;
      if (query.includes("SELECT 1")) {
        return [{ "?column?": 1 }] as T;
      }
      return [{ lag_ms: lagMs }] as T;
    },
  };

  return {
    client,
    setFail: (err: Error | null) => {
      fail = err;
    },
    setLag: (lag: number) => {
      lagMs = lag;
    },
  };
}

function mockExpressReqRes(method: string) {
  const req = {
    method,
    headers: {},
  } as unknown as Request;

  let statusCode = 200;
  let jsonBody: any = null;
  const headers: Record<string, string> = {};
  let nextCalled = false;

  const res = {
    status: (code: number) => {
      statusCode = code;
      return res;
    },
    setHeader: (name: string, value: string) => {
      headers[name.toLowerCase()] = value;
      return res;
    },
    json: (body: any) => {
      jsonBody = body;
      return res;
    },
  } as unknown as Response;

  const next = () => {
    nextCalled = true;
  };

  return {
    req,
    res,
    next,
    getStatusCode: () => statusCode,
    getJsonBody: () => jsonBody,
    getHeader: (name: string) => headers[name.toLowerCase()],
    isNextCalled: () => nextCalled,
  };
}

describe("Database Cluster Health Monitor & Primary Election (#218)", () => {
  let primaryMock: ReturnType<typeof mockDbNode>;
  let replicaEu: ReturnType<typeof mockDbNode>;
  let replicaUs: ReturnType<typeof mockDbNode>;
  let replicaApac: ReturnType<typeof mockDbNode>;
  let monitor: DatabaseClusterMonitor;

  beforeEach(() => {
    primaryMock = mockDbNode();
    replicaEu = mockDbNode({ lagMs: 150 });
    replicaUs = mockDbNode({ lagMs: 300 });
    replicaApac = mockDbNode({ lagMs: 1500 }); // Exceeds 1000ms threshold

    monitor = new DatabaseClusterMonitor({
      checkIntervalMs: 5000,
      maxLagMs: 1000,
      healthTimeoutMs: 1000,
      primary: { client: primaryMock.client, region: "eu-central" },
      replicas: [
        { region: "eu-west", client: replicaEu.client },
        { region: "us-east", client: replicaUs.client },
        { region: "ap-southeast", client: replicaApac.client },
      ],
    });
  });

  afterEach(() => {
    monitor.stop();
  });

  describe("Replication Lag & Node Isolation", () => {
    it("isolates replicas exceeding 1000ms replication lag threshold", async () => {
      await monitor.checkAll();
      const status = monitor.getClusterStatus();

      assert.equal(status.primary.healthy, true);
      assert.equal(status.isMaintenanceMode, false);

      const eu = status.replicas.find((r) => r.region === "eu-west")!;
      assert.equal(eu.healthy, true);
      assert.equal(eu.isolated, false);
      assert.equal(eu.lagMs, 150);

      const us = status.replicas.find((r) => r.region === "us-east")!;
      assert.equal(us.healthy, true);
      assert.equal(us.isolated, false);
      assert.equal(us.lagMs, 300);

      const apac = status.replicas.find((r) => r.region === "ap-southeast")!;
      assert.equal(apac.isolated, true);
      assert.equal(apac.healthy, false);
      assert.equal(apac.lagMs, 1500);

      const eligible = monitor.getEligibleReplicas();
      assert.equal(eligible.length, 2);
      assert.ok(!eligible.some((r) => r.region === "ap-southeast"));
    });

    it("restores isolated replica when lag drops below threshold", async () => {
      await monitor.checkAll();
      assert.equal(monitor.getClusterStatus().replicas.find((r) => r.region === "ap-southeast")!.isolated, true);

      // Lag drops to 200ms
      replicaApac.setLag(200);
      await monitor.checkReplicas();

      const apac = monitor.getClusterStatus().replicas.find((r) => r.region === "ap-southeast")!;
      assert.equal(apac.isolated, false);
      assert.equal(apac.healthy, true);
      assert.equal(apac.lagMs, 200);
    });
  });

  describe("Primary Failover & Candidate Election", () => {
    it("engages read-only maintenance mode when primary fails and elects lowest-lag replica", async () => {
      let failureFired = false;
      let electedRegion: string | null = null;

      const monitorWithHooks = new DatabaseClusterMonitor({
        primary: { client: primaryMock.client, region: "eu-central" },
        replicas: [
          { region: "us-east", client: replicaUs.client },
          { region: "eu-west", client: replicaEu.client },
          { region: "ap-southeast", client: replicaApac.client }, // isolated (>1000ms)
        ],
        maxLagMs: 1000,
        onPrimaryFailure: () => {
          failureFired = true;
        },
        onCandidateElected: (cand) => {
          electedRegion = cand;
        },
      });

      // Initially healthy
      await monitorWithHooks.checkAll();
      assert.equal(monitorWithHooks.getIsMaintenanceMode(), false);

      // Primary connection drops
      primaryMock.setFail(new Error("Connection terminated unexpectedly"));
      await monitorWithHooks.checkPrimary();

      assert.equal(monitorWithHooks.getIsMaintenanceMode(), true);
      assert.equal(monitorWithHooks.isReadOnly(), true);
      assert.equal(failureFired, true);

      // Lowest lag eligible candidate (eu-west has 150ms, us-east has 300ms, ap-southeast is isolated)
      assert.equal(monitorWithHooks.getElectedCandidate(), "eu-west");
      assert.equal(electedRegion, "eu-west");
    });

    it("automatically disengages maintenance mode when primary connection restores", async () => {
      let recoveryFired = false;
      const monitorWithHooks = new DatabaseClusterMonitor({
        primary: { client: primaryMock.client },
        replicas: [{ region: "eu-west", client: replicaEu.client }],
        onPrimaryRecovery: () => {
          recoveryFired = true;
        },
      });

      // Primary drops
      primaryMock.setFail(new Error("ECONNREFUSED"));
      await monitorWithHooks.checkAll();
      assert.equal(monitorWithHooks.getIsMaintenanceMode(), true);

      // Primary restores
      primaryMock.setFail(null);
      await monitorWithHooks.checkPrimary();

      assert.equal(monitorWithHooks.getIsMaintenanceMode(), false);
      assert.equal(monitorWithHooks.isReadOnly(), false);
      assert.equal(monitorWithHooks.getElectedCandidate(), null);
      assert.equal(recoveryFired, true);
    });
  });

  describe("databaseMaintenanceMiddleware", () => {
    it("allows all requests through when cluster is healthy", () => {
      const middleware = createDatabaseMaintenanceMiddleware(monitor);
      const { req, res, next, isNextCalled } = mockExpressReqRes("POST");

      middleware(req, res, next);
      assert.equal(isNextCalled(), true);
    });

    it("allows GET and HEAD requests through even during maintenance mode", async () => {
      primaryMock.setFail(new Error("Primary down"));
      await monitor.checkPrimary();
      assert.equal(monitor.getIsMaintenanceMode(), true);

      const middleware = createDatabaseMaintenanceMiddleware(monitor);

      const getCheck = mockExpressReqRes("GET");
      middleware(getCheck.req, getCheck.res, getCheck.next);
      assert.equal(getCheck.isNextCalled(), true);

      const headCheck = mockExpressReqRes("HEAD");
      middleware(headCheck.req, headCheck.res, headCheck.next);
      assert.equal(headCheck.isNextCalled(), true);
    });

    it("blocks write mutations during maintenance mode with 503 and Retry-After: 30", async () => {
      primaryMock.setFail(new Error("Primary down"));
      await monitor.checkAll();
      assert.equal(monitor.getIsMaintenanceMode(), true);

      const middleware = createDatabaseMaintenanceMiddleware(monitor);

      for (const method of ["POST", "PUT", "PATCH", "DELETE"]) {
        const check = mockExpressReqRes(method);
        middleware(check.req, check.res, check.next);

        assert.equal(check.isNextCalled(), false, `${method} should be blocked`);
        assert.equal(check.getStatusCode(), 503);
        assert.equal(check.getHeader("Retry-After"), "30");
        assert.equal(check.getJsonBody().readOnly, true);
        assert.equal(check.getJsonBody().retryAfter, 30);
        assert.equal(check.getJsonBody().electedCandidate, "eu-west");
      }
    });
  });
});
