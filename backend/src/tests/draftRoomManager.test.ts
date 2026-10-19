import { describe, it, beforeEach } from "node:test";
import assert from "node:assert";
import {
  DraftService,
  DraftRoomManager,
  PlayerCandidate,
  SQUAD_POSITION_LIMITS,
} from "../services/draft/draftService.js";

describe("DraftRoomManager Real-Time Snake Draft Engine (Issue 222)", () => {
  let draftService: DraftService;

  const generatePlayerPool = (): PlayerCandidate[] => {
    const pool: PlayerCandidate[] = [];
    let id = 1;

    // 10 GKPs
    for (let i = 1; i <= 10; i++) {
      pool.push({
        id: id++,
        displayName: `Goalkeeper ${i}`,
        position: "GKP",
        price: 4.0 + i * 0.2,
        totalPoints: 50 + i * 10,
      });
    }

    // 25 DEFs
    for (let i = 1; i <= 25; i++) {
      pool.push({
        id: id++,
        displayName: `Defender ${i}`,
        position: "DEF",
        price: 4.5 + i * 0.2,
        totalPoints: 60 + i * 8,
      });
    }

    // 25 MIDs
    for (let i = 1; i <= 25; i++) {
      pool.push({
        id: id++,
        displayName: `Midfielder ${i}`,
        position: "MID",
        price: 5.0 + i * 0.3,
        totalPoints: 70 + i * 9,
      });
    }

    // 15 FWDs
    for (let i = 1; i <= 15; i++) {
      pool.push({
        id: id++,
        displayName: `Forward ${i}`,
        position: "FWD",
        price: 6.0 + i * 0.4,
        totalPoints: 80 + i * 12,
      });
    }

    return pool;
  };

  beforeEach(() => {
    draftService = new DraftService();
  });

  it("should generate correct snake draft sequence alternating across rounds", () => {
    const participants = ["squad-1", "squad-2", "squad-3"];
    const sequence = DraftRoomManager.buildSnakePickOrder(participants, 4);

    assert.strictEqual(sequence.length, 12);

    // Round 1 (odd): 1 -> 2 -> 3
    assert.strictEqual(sequence[0].squadId, "squad-1");
    assert.strictEqual(sequence[1].squadId, "squad-2");
    assert.strictEqual(sequence[2].squadId, "squad-3");

    // Round 2 (even): 3 -> 2 -> 1
    assert.strictEqual(sequence[3].squadId, "squad-3");
    assert.strictEqual(sequence[4].squadId, "squad-2");
    assert.strictEqual(sequence[5].squadId, "squad-1");

    // Round 3 (odd): 1 -> 2 -> 3
    assert.strictEqual(sequence[6].squadId, "squad-1");
    assert.strictEqual(sequence[7].squadId, "squad-2");
    assert.strictEqual(sequence[8].squadId, "squad-3");

    // Round 4 (even): 3 -> 2 -> 1
    assert.strictEqual(sequence[9].squadId, "squad-3");
    assert.strictEqual(sequence[10].squadId, "squad-2");
    assert.strictEqual(sequence[11].squadId, "squad-1");
  });

  it("should broadcast timer ticks and auto-pick highest-ranked eligible player when timer expires", async () => {
    const leagueId = "league-auto-pick-test";
    const participants = ["squad-alpha", "squad-beta"];
    const players = generatePlayerPool();

    const emittedEvents: any[] = [];
    const manager = new DraftRoomManager({
      leagueId,
      participants,
      rounds: 2,
      timePerPickSec: 3,
      availablePlayers: players,
      draftService,
      broadcastCallback: (e) => emittedEvents.push(e),
    });

    manager.start();
    assert.strictEqual(manager.status, "IN_PROGRESS");

    // Tick 1
    await manager.tick();
    assert.strictEqual(manager.remainingSeconds, 2);

    // Tick 2
    await manager.tick();
    assert.strictEqual(manager.remainingSeconds, 1);

    // Tick 3: reaches 0 -> triggers auto-pick
    await manager.tick();

    // Verify auto-pick executed for squad-alpha
    assert.strictEqual(manager.currentPickIndex, 1);
    const alphaPicks = manager.getSquadPicks("squad-alpha");
    assert.strictEqual(alphaPicks.length, 1);

    // Top player in pool by totalPoints is Midfielder 25 (70 + 25*9 = 295)
    assert.strictEqual(alphaPicks[0].displayName, "Midfielder 25");

    // Check emitted events
    const ticks = emittedEvents.filter((e) => e.type === "DRAFT_TIMER_TICK");
    assert.strictEqual(ticks.length, 3);
    const autoPicks = emittedEvents.filter((e) => e.type === "AUTO_PICK_EXECUTED");
    assert.strictEqual(autoPicks.length, 1);

    manager.stop();
  });

  it("should enforce position quotas and prevent drafting beyond limits", async () => {
    const leagueId = "league-quota-test";
    const participants = ["squad-1", "squad-2"];
    const players = generatePlayerPool();

    const manager = new DraftRoomManager({
      leagueId,
      participants,
      rounds: 15,
      timePerPickSec: 60,
      availablePlayers: players,
      draftService,
    });

    const gkpPlayers = players.filter((p) => p.position === "GKP");

    // Round 1: squad-1 picks GKP 1
    await manager.makePick("squad-1", gkpPlayers[0].id);
    // Round 1: squad-2 picks GKP 2
    await manager.makePick("squad-2", gkpPlayers[1].id);

    // Round 2 (snake): squad-2 picks GKP 3
    await manager.makePick("squad-2", gkpPlayers[2].id);
    // Round 2: squad-1 picks GKP 4
    await manager.makePick("squad-1", gkpPlayers[3].id);

    // Now squad-1 has 2 GKPs (max quota: 2)
    assert.strictEqual(manager.getSquadPositionCounts("squad-1").GKP, 2);

    // Round 3: squad-1's turn. Attempting to pick a 3rd GKP must fail
    await assert.rejects(
      async () => manager.makePick("squad-1", gkpPlayers[4].id),
      /position quota exceeded/
    );

    manager.stop();
  });

  it("should simulate a full 15-round draft with manual picks and timeouts, producing valid 15-man rosters", async () => {
    const leagueId = "league-full-15-round";
    const participants = ["manager-A", "manager-B", "manager-C"];
    const players = generatePlayerPool();

    const events: any[] = [];
    const manager = new DraftRoomManager({
      leagueId,
      participants,
      rounds: 15,
      timePerPickSec: 60,
      availablePlayers: players,
      draftService,
      broadcastCallback: (e) => events.push(e),
    });

    manager.start();

    // 3 managers x 15 rounds = 45 picks
    // Mix manual picks and auto-picks (simulating timeout)
    while (manager.status === "IN_PROGRESS") {
      const turn = manager.getCurrentTurn();
      if (!turn) break;

      // Every 3rd pick simulates an expired timer (auto-pick)
      if (turn.pickNumber % 3 === 0) {
        await manager.autoPickCurrentTurn();
      } else {
        // Manual pick of highest-ranked eligible player
        const squadId = turn.squadId;
        const eligible = players
          .filter((p) => draftService.getPlayerOwner(leagueId, p.id) === null)
          .filter((p) => manager.isPlayerEligibleForSquad(squadId, p))
          .sort((a, b) => b.totalPoints - a.totalPoints);

        assert.ok(eligible.length > 0, "Must have eligible player");
        await manager.makePick(squadId, eligible[0].id);
      }
    }

    assert.strictEqual(manager.status, "COMPLETED");
    assert.strictEqual(manager.currentPickIndex, 45);

    // Assert each manager ended up with exactly 15 players adhering to standard FPL position composition
    for (const p of participants) {
      const roster = manager.getSquadPicks(p);
      assert.strictEqual(roster.length, 15, `${p} must have exactly 15 players`);

      const counts = manager.getSquadPositionCounts(p);
      assert.strictEqual(counts.GKP, SQUAD_POSITION_LIMITS.GKP, `${p} must have 2 GKPs`);
      assert.strictEqual(counts.DEF, SQUAD_POSITION_LIMITS.DEF, `${p} must have 5 DEFs`);
      assert.strictEqual(counts.MID, SQUAD_POSITION_LIMITS.MID, `${p} must have 5 MIDs`);
      assert.strictEqual(counts.FWD, SQUAD_POSITION_LIMITS.FWD, `${p} must have 3 FWDs`);
    }

    // Verify DRAFT_COMPLETED event emitted
    const completedEvent = events.find((e) => e.type === "DRAFT_COMPLETED");
    assert.ok(completedEvent);
    assert.strictEqual(completedEvent.totalPicks, 45);

    manager.stop();
  });
});
