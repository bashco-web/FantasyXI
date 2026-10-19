import { describe, it, beforeEach } from "node:test";
import assert from "node:assert/strict";
import {
  LiveScoreIngestionDaemon,
  LiveStatsPollPlayer,
  LiveWebSocketGateway,
} from "../services/live/liveTimelineService.js";
import { Position } from "../types/index.js";

class MockRedis {
  public store = new Map<string, string>();
  async get(key: string): Promise<string | null> {
    return this.store.get(key) ?? null;
  }
  async set(key: string, value: string): Promise<unknown> {
    this.store.set(key, value);
    return "OK";
  }
  async del(...keys: string[]): Promise<number> {
    let count = 0;
    for (const k of keys) {
      if (this.store.delete(k)) count++;
    }
    return count;
  }
  async keys(pattern: string): Promise<string[]> {
    return Array.from(this.store.keys());
  }
}

class MockGateway implements LiveWebSocketGateway {
  public broadcasts: Array<{ event: string; payload: any }> = [];
  broadcast(event: string, payload: any) {
    this.broadcasts.push({ event, payload });
  }
}

function createMockDb() {
  return {
    squadPlayer: {
      findMany: async ({ where }: any) => {
        const playerIds = where.playerId.in;
        // Squad 1 has player 10 (FWD) and player 11 (MID)
        const memberships = [
          { squadId: "squad-alpha", playerId: 10 },
          { squadId: "squad-alpha", playerId: 11 },
          { squadId: "squad-beta", playerId: 11 },
        ];
        return memberships.filter((m) => playerIds.includes(m.playerId));
      },
    },
    squad: {
      findMany: async ({ where }: any) => {
        const squadIds: string[] = where.id.in;
        const allSquads: Record<string, any> = {
          "squad-alpha": {
            id: "squad-alpha",
            name: "Alpha Squad",
            players: [
              {
                squadId: "squad-alpha",
                playerId: 10,
                isStarter: true,
                isCaptain: true,
                isViceCaptain: false,
                positionOrder: 1,
                player: { id: 10, position: Position.FWD, team: { shortName: "ARS" } },
              },
              {
                squadId: "squad-alpha",
                playerId: 11,
                isStarter: true,
                isCaptain: false,
                isViceCaptain: true,
                positionOrder: 2,
                player: { id: 11, position: Position.MID, team: { shortName: "MCI" } },
              },
            ],
          },
          "squad-beta": {
            id: "squad-beta",
            name: "Beta Squad",
            players: [
              {
                squadId: "squad-beta",
                playerId: 11,
                isStarter: true,
                isCaptain: false,
                isViceCaptain: false,
                positionOrder: 1,
                player: { id: 11, position: Position.MID, team: { shortName: "MCI" } },
              },
            ],
          },
        };
        return squadIds.map((id) => allSquads[id]).filter(Boolean);
      },
    },
  };
}

describe("LiveScoreIngestionDaemon", () => {
  let daemon: LiveScoreIngestionDaemon;
  let mockRedis: MockRedis;
  let mockGateway: MockGateway;
  let mockDb: any;

  beforeEach(() => {
    mockRedis = new MockRedis();
    mockGateway = new MockGateway();
    mockDb = createMockDb();
    daemon = new LiveScoreIngestionDaemon({
      db: mockDb,
      redis: mockRedis as any,
      gateway: mockGateway,
      pollIntervalMs: 1000,
    });
  });

  it("calculates diffs between consecutive polls and emits MATCH_EVENT_OCCURRED", async () => {
    const initialPoll: LiveStatsPollPlayer[] = [
      {
        playerId: 10,
        gameweekId: 1,
        fixtureId: 101,
        minutes: 45,
        goals: 0,
        assists: 0,
        yellowCards: 0,
        redCards: 0,
        saves: 0,
        bonus: 0,
        totalPoints: 1,
        player: {
          id: 10,
          displayName: "Haaland",
          position: Position.FWD,
          team: { shortName: "MCI" },
        },
      },
    ];

    // First ingestion establishes baseline
    const result1 = await daemon.processStatsUpdate(1, initialPoll);
    assert.equal(result1.matchEvents.length, 0);

    // Second poll: Haaland scores 2 goals and gets 1 yellow card
    const secondPoll: LiveStatsPollPlayer[] = [
      {
        playerId: 10,
        gameweekId: 1,
        fixtureId: 101,
        minutes: 70,
        goals: 2,
        assists: 1,
        yellowCards: 1,
        redCards: 0,
        saves: 0,
        bonus: 0,
        totalPoints: 14,
        player: {
          id: 10,
          displayName: "Haaland",
          position: Position.FWD,
          team: { shortName: "MCI" },
        },
      },
    ];

    const result2 = await daemon.processStatsUpdate(1, secondPoll);
    assert.equal(result2.matchEvents.length, 4); // 2 goals + 1 assist + 1 yc

    // Check goals
    const goals = result2.matchEvents.filter((e) => e.type === "GOAL");
    assert.equal(goals.length, 2);
    assert.equal(goals[0].id, "101-10-goal-1");
    assert.equal(goals[1].id, "101-10-goal-2");
    assert.equal(goals[0].pointsAwarded, 4); // FWD goal = 4 pts
    assert.equal(goals[0].playerName, "Haaland");

    // Check assist
    const assists = result2.matchEvents.filter((e) => e.type === "ASSIST");
    assert.equal(assists.length, 1);
    assert.equal(assists[0].pointsAwarded, 3);

    // Check yellow card
    const ycs = result2.matchEvents.filter((e) => e.type === "YELLOW_CARD");
    assert.equal(ycs.length, 1);
    assert.equal(ycs[0].pointsAwarded, -1);

    // Check WebSocket broadcasts
    const matchEventBroadcasts = mockGateway.broadcasts.filter(
      (b) => b.event === "MATCH_EVENT_OCCURRED"
    );
    assert.equal(matchEventBroadcasts.length, 4);
    assert.equal(matchEventBroadcasts[0].payload.id, "101-10-goal-1");
  });

  it("prevents duplicate emissions when polls repeat the same stat counts", async () => {
    const poll: LiveStatsPollPlayer[] = [
      {
        playerId: 11,
        gameweekId: 1,
        fixtureId: 102,
        minutes: 60,
        goals: 1,
        assists: 0,
        yellowCards: 0,
        redCards: 0,
        saves: 0,
        bonus: 0,
        totalPoints: 6,
        player: {
          id: 11,
          displayName: "Saka",
          position: Position.MID,
          team: { shortName: "ARS" },
        },
      },
    ];

    const result1 = await daemon.processStatsUpdate(1, poll);
    assert.equal(result1.matchEvents.length, 1);
    assert.equal(result1.matchEvents[0].pointsAwarded, 5); // MID goal = 5 pts

    // Identical poll: no new events emitted
    const result2 = await daemon.processStatsUpdate(1, poll);
    assert.equal(result2.matchEvents.length, 0);

    // Total WebSocket MATCH_EVENT_OCCURRED broadcast should still be 1
    const matchEventBroadcasts = mockGateway.broadcasts.filter(
      (b) => b.event === "MATCH_EVENT_OCCURRED"
    );
    assert.equal(matchEventBroadcasts.length, 1);
  });

  it("recomputes live squad scores, caches in Redis, and dispatches SQUAD_SCORE_UPDATED", async () => {
    const poll: LiveStatsPollPlayer[] = [
      {
        playerId: 10,
        gameweekId: 1,
        fixtureId: 101,
        minutes: 90,
        goals: 1,
        assists: 0,
        yellowCards: 0,
        redCards: 0,
        saves: 0,
        bonus: 3,
        totalPoints: 10,
        player: {
          id: 10,
          displayName: "Haaland",
          position: Position.FWD,
          team: { shortName: "MCI" },
        },
      },
    ];

    const result = await daemon.processStatsUpdate(1, poll);
    assert.ok(result.updatedSquadIds.includes("squad-alpha"));

    // Check Redis cache
    const cached = await daemon.getCachedSquadScore("squad-alpha", 1);
    assert.ok(cached !== null);
    assert.equal(cached.squadId, "squad-alpha");
    assert.equal(cached.gameweekId, 1);
    // Haaland raw is 10, bonus captain portion is 10, totalPoints is 20
    assert.equal(cached.captainPoints, 10);
    assert.equal(cached.totalPoints, 20);

    // Check SQUAD_SCORE_UPDATED broadcast
    const squadBroadcasts = mockGateway.broadcasts.filter(
      (b) => b.event === "SQUAD_SCORE_UPDATED"
    );
    assert.ok(squadBroadcasts.length >= 1);
    const alphaBroadcast = squadBroadcasts.find((b) => b.payload.squadId === "squad-alpha");
    assert.ok(alphaBroadcast);
    assert.equal(alphaBroadcast.payload.totalPoints, 20);
  });

  it("handles goalkeeper saves and bonus points deltas properly", async () => {
    const initial: LiveStatsPollPlayer[] = [
      {
        playerId: 1,
        gameweekId: 1,
        fixtureId: 105,
        minutes: 60,
        goals: 0,
        assists: 0,
        yellowCards: 0,
        redCards: 0,
        saves: 2,
        bonus: 0,
        totalPoints: 2,
        player: {
          id: 1,
          displayName: "Raya",
          position: Position.GKP,
          team: { shortName: "ARS" },
        },
      },
    ];

    await daemon.processStatsUpdate(1, initial);

    // Raya reaches 6 saves (2 extra save points) + 2 bonus
    const update: LiveStatsPollPlayer[] = [
      {
        playerId: 1,
        gameweekId: 1,
        fixtureId: 105,
        minutes: 90,
        goals: 0,
        assists: 0,
        yellowCards: 0,
        redCards: 0,
        saves: 6,
        bonus: 2,
        totalPoints: 6,
        player: {
          id: 1,
          displayName: "Raya",
          position: Position.GKP,
          team: { shortName: "ARS" },
        },
      },
    ];

    const result = await daemon.processStatsUpdate(1, update);
    const saveEvents = result.matchEvents.filter((e) => e.type === "SAVE");
    assert.equal(saveEvents.length, 1);
    assert.equal(saveEvents[0].pointsAwarded, 2); // Math.floor(6/3) - Math.floor(2/3) = 2 - 0 = 2

    const bonusEvents = result.matchEvents.filter((e) => e.type === "BONUS");
    assert.equal(bonusEvents.length, 1);
    assert.equal(bonusEvents[0].pointsAwarded, 2);
  });
});
