/**
 * Strict Draft Mode Service (Issue 160)
 * Enforces mutual exclusivity of player ownership per league with concurrency locking
 * and automated fallback pick algorithm.
 */

export interface LeaguePlayerOwnership {
  id: string;
  leagueId: string;
  playerId: number;
  squadId: string;
  roundDrafted: number;
  pickNumber: number;
  draftedAt: Date;
}

export interface PlayerCandidate {
  id: number;
  displayName: string;
  position: "GKP" | "DEF" | "MID" | "FWD";
  price: number;
  totalPoints: number;
}

export interface DraftRoomState {
  leagueId: string;
  status: "NOT_STARTED" | "IN_PROGRESS" | "COMPLETED";
  squadOrder: string[]; // List of squad IDs in draft order
  currentPickIndex: number; // 0 to (totalPicks - 1)
  currentRound: number;
  currentTurnSquadId: string;
  turnDeadline: Date;
  timePerPickSec: number;
  picks: LeaguePlayerOwnership[];
}

export class PlayerAlreadyDraftedError extends Error {
  public leagueId: string;
  public playerId: number;
  public existingSquadId: string;

  constructor(leagueId: string, playerId: number, existingSquadId: string) {
    super(`Player ${playerId} is already owned by squad ${existingSquadId} in league ${leagueId}`);
    this.name = "PlayerAlreadyDraftedError";
    this.leagueId = leagueId;
    this.playerId = playerId;
    this.existingSquadId = existingSquadId;
  }
}

export class NotYourTurnError extends Error {
  constructor(squadId: string, currentTurnSquadId: string) {
    super(`It is not squad ${squadId}'s turn to draft (active turn: ${currentTurnSquadId})`);
    this.name = "NotYourTurnError";
  }
}

// In-memory mutex map to guarantee strict transactional concurrency per league
const leagueMutexes: Map<string, Promise<unknown>> = new Map();

async function withLeagueLock<T>(leagueId: string, fn: () => Promise<T>): Promise<T> {
  const currentLock = leagueMutexes.get(leagueId) || Promise.resolve();
  let release: () => void;
  const nextLock = new Promise<void>((resolve) => {
    release = resolve;
  });

  leagueMutexes.set(leagueId, nextLock);

  try {
    await currentLock;
    return await fn();
  } finally {
    release!();
    if (leagueMutexes.get(leagueId) === nextLock) {
      leagueMutexes.delete(leagueId);
    }
  }
}

export class DraftService {
  // Store ownership map: `leagueId:playerId` -> LeaguePlayerOwnership
  private ownershipMap: Map<string, LeaguePlayerOwnership> = new Map();
  private draftRooms: Map<string, DraftRoomState> = new Map();
  private listeners: Map<string, Set<(event: any) => void>> = new Map();

  public createDraftRoom(
    leagueId: string,
    squadOrder: string[],
    timePerPickSec: number = 60
  ): DraftRoomState {
    const state: DraftRoomState = {
      leagueId,
      status: "IN_PROGRESS",
      squadOrder,
      currentPickIndex: 0,
      currentRound: 1,
      currentTurnSquadId: squadOrder[0],
      turnDeadline: new Date(Date.now() + timePerPickSec * 1000),
      timePerPickSec,
      picks: [],
    };
    this.draftRooms.set(leagueId, state);
    return state;
  }

  public getDraftRoom(leagueId: string): DraftRoomState | undefined {
    return this.draftRooms.get(leagueId);
  }

  public subscribe(leagueId: string, listener: (event: any) => void): () => void {
    if (!this.listeners.has(leagueId)) {
      this.listeners.set(leagueId, new Set());
    }
    this.listeners.get(leagueId)!.add(listener);
    return () => {
      this.listeners.get(leagueId)?.delete(listener);
    };
  }

  private broadcast(leagueId: string, event: Record<string, unknown>): void {
    const roomListeners = this.listeners.get(leagueId);
    if (roomListeners) {
      for (const listener of roomListeners) {
        listener(event);
      }
    }
  }

  /**
   * Executes a draft pick with atomic concurrency locking.
   * Throws PlayerAlreadyDraftedError if the player is already owned in this league.
   */
  public async executeDraftPick(
    leagueId: string,
    squadId: string,
    playerId: number
  ): Promise<LeaguePlayerOwnership> {
    return withLeagueLock(leagueId, async () => {
      const room = this.draftRooms.get(leagueId);
      if (room && room.status === "IN_PROGRESS") {
        if (room.currentTurnSquadId !== squadId) {
          throw new NotYourTurnError(squadId, room.currentTurnSquadId);
        }
      }

      // 1. Strict mutual exclusivity check within this league
      const key = `${leagueId}:${playerId}`;
      const existing = this.ownershipMap.get(key);
      if (existing) {
        throw new PlayerAlreadyDraftedError(leagueId, playerId, existing.squadId);
      }

      const pickNumber = room ? room.picks.length + 1 : 1;
      const round = room ? Math.floor((pickNumber - 1) / room.squadOrder.length) + 1 : 1;

      const record: LeaguePlayerOwnership = {
        id: `own_${Date.now()}_${Math.random().toString(36).slice(2, 6)}`,
        leagueId,
        playerId,
        squadId,
        roundDrafted: round,
        pickNumber,
        draftedAt: new Date(),
      };

      this.ownershipMap.set(key, record);

      if (room) {
        room.picks.push(record);
        // Advance snake/round-robin turn
        const nextIndex = room.currentPickIndex + 1;
        const totalSquads = room.squadOrder.length;
        const nextRound = Math.floor(nextIndex / totalSquads) + 1;

        // Snake draft turn calculation: reverse order on even rounds
        let nextSquadId: string;
        const roundIndex = nextIndex % totalSquads;
        if (nextRound % 2 === 1) {
          nextSquadId = room.squadOrder[roundIndex];
        } else {
          nextSquadId = room.squadOrder[totalSquads - 1 - roundIndex];
        }

        room.currentPickIndex = nextIndex;
        room.currentRound = nextRound;
        room.currentTurnSquadId = nextSquadId;
        room.turnDeadline = new Date(Date.now() + room.timePerPickSec * 1000);

        this.broadcast(leagueId, {
          type: "DRAFT_PICK_MADE",
          pick: record,
          nextTurn: {
            squadId: nextSquadId,
            round: nextRound,
            pickIndex: nextIndex,
            deadline: room.turnDeadline,
          },
        });
      }

      return record;
    });
  }

  /**
   * Fallback auto-pick algorithm if manager misses their timer:
   * Selects highest-value available player (by totalPoints or price) not yet drafted in league.
   */
  public async executeAutoPick(
    leagueId: string,
    squadId: string,
    availablePlayers: PlayerCandidate[]
  ): Promise<LeaguePlayerOwnership> {
    const unpicked = availablePlayers
      .filter((p) => !this.ownershipMap.has(`${leagueId}:${p.id}`))
      .sort((a, b) => b.totalPoints - a.totalPoints || b.price - a.price);

    if (unpicked.length === 0) {
      throw new Error(`No available players left to auto-pick for league ${leagueId}`);
    }

    const highestValuePlayer = unpicked[0];
    const pick = await this.executeDraftPick(leagueId, squadId, highestValuePlayer.id);

    this.broadcast(leagueId, {
      type: "AUTO_PICK_EXECUTED",
      squadId,
      player: highestValuePlayer,
      pick,
    });

    return pick;
  }

  public getPlayerOwner(leagueId: string, playerId: number): string | null {
    return this.ownershipMap.get(`${leagueId}:${playerId}`)?.squadId || null;
  }

  public getLeagueOwnerships(leagueId: string): LeaguePlayerOwnership[] {
    const list: LeaguePlayerOwnership[] = [];
    for (const [k, v] of this.ownershipMap.entries()) {
      if (k.startsWith(`${leagueId}:`)) list.push(v);
    }
    return list;
  }

  public clear(): void {
    this.ownershipMap.clear();
    this.draftRooms.clear();
    this.listeners.clear();
  }
}

export const draftService = new DraftService();

export const SQUAD_POSITION_LIMITS = {
  GKP: 2,
  DEF: 5,
  MID: 5,
  FWD: 3,
} as const;

export interface DraftRoomConfig {
  leagueId: string;
  participants: string[];
  rounds?: number;
  timePerPickSec?: number;
  availablePlayers: PlayerCandidate[];
  broadcastCallback?: (event: any) => void;
  draftService?: DraftService;
}

export class DraftRoomManager {
  public leagueId: string;
  public participants: string[];
  public rounds: number;
  public timePerPickSec: number;
  public availablePlayers: PlayerCandidate[];
  public draftSequence: Array<{ squadId: string; round: number; pickNumber: number }>;
  public currentPickIndex: number = 0;
  public remainingSeconds: number;
  public status: "NOT_STARTED" | "IN_PROGRESS" | "COMPLETED" = "NOT_STARTED";
  private squadDraftedPlayers: Map<string, PlayerCandidate[]> = new Map();
  private timer: NodeJS.Timeout | null = null;
  private broadcastCallback?: (event: any) => void;
  private draftService: DraftService;

  constructor(config: DraftRoomConfig) {
    this.leagueId = config.leagueId;
    this.participants = [...config.participants];
    this.rounds = config.rounds ?? 15;
    this.timePerPickSec = config.timePerPickSec ?? 60;
    this.availablePlayers = [...config.availablePlayers];
    this.broadcastCallback = config.broadcastCallback;
    this.draftService = config.draftService ?? draftService;
    this.remainingSeconds = this.timePerPickSec;

    for (const p of this.participants) {
      this.squadDraftedPlayers.set(p, []);
    }

    this.draftSequence = DraftRoomManager.buildSnakePickOrder(this.participants, this.rounds);
    this.draftService.createDraftRoom(this.leagueId, this.participants, this.timePerPickSec);
  }

  public static buildSnakePickOrder(
    participants: string[],
    totalRounds: number = 15
  ): Array<{ squadId: string; round: number; pickNumber: number }> {
    const sequence: Array<{ squadId: string; round: number; pickNumber: number }> = [];
    let pickNumber = 1;
    const n = participants.length;
    for (let round = 1; round <= totalRounds; round++) {
      if (round % 2 === 1) {
        // Odd round: 1 -> N
        for (let i = 0; i < n; i++) {
          sequence.push({ squadId: participants[i], round, pickNumber: pickNumber++ });
        }
      } else {
        // Even round: N -> 1
        for (let i = n - 1; i >= 0; i--) {
          sequence.push({ squadId: participants[i], round, pickNumber: pickNumber++ });
        }
      }
    }
    return sequence;
  }

  public getCurrentTurn(): { squadId: string; round: number; pickNumber: number } | null {
    if (this.currentPickIndex >= this.draftSequence.length) return null;
    return this.draftSequence[this.currentPickIndex];
  }

  public start(): void {
    if (this.status === "COMPLETED") return;
    this.status = "IN_PROGRESS";
    this.remainingSeconds = this.timePerPickSec;
    this.startTimer();
    this.emitEvent({
      type: "DRAFT_STARTED",
      leagueId: this.leagueId,
      currentTurn: this.getCurrentTurn(),
    });
  }

  public stop(): void {
    this.stopTimer();
  }

  public startTimer(): void {
    this.stopTimer();
    this.timer = setInterval(() => {
      this.tick();
    }, 1000);
    if (this.timer && typeof this.timer.unref === "function") {
      this.timer.unref();
    }
  }

  public stopTimer(): void {
    if (this.timer) {
      clearInterval(this.timer);
      this.timer = null;
    }
  }

  public async tick(): Promise<void> {
    if (this.status !== "IN_PROGRESS") return;

    this.remainingSeconds--;
    const turn = this.getCurrentTurn();
    this.emitEvent({
      type: "DRAFT_TIMER_TICK",
      leagueId: this.leagueId,
      squadId: turn?.squadId,
      remainingSeconds: this.remainingSeconds,
      round: turn?.round,
      pickIndex: this.currentPickIndex,
    });

    if (this.remainingSeconds <= 0) {
      await this.autoPickCurrentTurn();
    }
  }

  public isPlayerEligibleForSquad(squadId: string, player: PlayerCandidate): boolean {
    const owned = this.squadDraftedPlayers.get(squadId) || [];
    const pos = player.position;
    const count = owned.filter((p) => p.position === pos).length;
    const max = SQUAD_POSITION_LIMITS[pos];
    return count < max;
  }

  public getSquadPositionCounts(squadId: string): Record<string, number> {
    const owned = this.squadDraftedPlayers.get(squadId) || [];
    return {
      GKP: owned.filter((p) => p.position === "GKP").length,
      DEF: owned.filter((p) => p.position === "DEF").length,
      MID: owned.filter((p) => p.position === "MID").length,
      FWD: owned.filter((p) => p.position === "FWD").length,
    };
  }

  public getSquadPicks(squadId: string): PlayerCandidate[] {
    return this.squadDraftedPlayers.get(squadId) || [];
  }

  public async makePick(squadId: string, playerId: number): Promise<LeaguePlayerOwnership> {
    const currentTurn = this.getCurrentTurn();
    if (!currentTurn) {
      throw new Error("Draft already completed");
    }
    if (currentTurn.squadId !== squadId) {
      throw new NotYourTurnError(squadId, currentTurn.squadId);
    }

    const player = this.availablePlayers.find((p) => p.id === playerId);
    if (!player) {
      throw new Error(`Player ${playerId} not found in available player pool`);
    }

    if (!this.isPlayerEligibleForSquad(squadId, player)) {
      throw new Error(
        `Squad ${squadId} cannot draft player ${player.displayName} (${player.position}) - position quota exceeded`
      );
    }

    const pick = await this.draftService.executeDraftPick(this.leagueId, squadId, playerId);
    this.squadDraftedPlayers.get(squadId)?.push(player);

    this.currentPickIndex++;
    this.remainingSeconds = this.timePerPickSec;

    if (this.currentPickIndex >= this.draftSequence.length) {
      this.status = "COMPLETED";
      this.stopTimer();
      this.emitEvent({
        type: "DRAFT_COMPLETED",
        leagueId: this.leagueId,
        totalPicks: this.draftSequence.length,
      });
    } else {
      this.emitEvent({
        type: "DRAFT_PICK_CONFIRMED",
        leagueId: this.leagueId,
        pick,
        player,
        nextTurn: this.getCurrentTurn(),
      });
    }

    return pick;
  }

  public async autoPickCurrentTurn(): Promise<LeaguePlayerOwnership> {
    const currentTurn = this.getCurrentTurn();
    if (!currentTurn || this.status === "COMPLETED") {
      throw new Error("No active turn to auto-pick");
    }

    const squadId = currentTurn.squadId;
    const eligible = this.availablePlayers
      .filter((p) => this.draftService.getPlayerOwner(this.leagueId, p.id) === null)
      .filter((p) => this.isPlayerEligibleForSquad(squadId, p))
      .sort((a, b) => b.totalPoints - a.totalPoints || b.price - a.price);

    if (eligible.length === 0) {
      throw new Error(`No eligible players matching squad position constraints for squad ${squadId}`);
    }

    const bestPlayer = eligible[0];
    const pick = await this.makePick(squadId, bestPlayer.id);

    this.emitEvent({
      type: "AUTO_PICK_EXECUTED",
      leagueId: this.leagueId,
      squadId,
      player: bestPlayer,
      pick,
    });

    return pick;
  }

  private emitEvent(event: Record<string, unknown>): void {
    if (this.broadcastCallback) {
      this.broadcastCallback(event);
    }
  }
}
