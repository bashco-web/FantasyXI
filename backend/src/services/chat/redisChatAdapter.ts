import { randomUUID } from "node:crypto";
import { type Redis as RedisClient } from "ioredis";
import { createRedisClient, getRedisConfig } from "../../config/redis.js";
import type { ChatHub, ChatMessageDto, ChatPubSubAdapter } from "./chatService.js";

export const LEAGUE_CHAT_CHANNEL_PREFIX = "league:chat:";

export function getLeagueChatChannel(leagueId: string): string {
  return `${LEAGUE_CHAT_CHANNEL_PREFIX}${leagueId}`;
}

export function parseLeagueIdFromChannel(channel: string): string | null {
  if (channel.startsWith(LEAGUE_CHAT_CHANNEL_PREFIX)) {
    return channel.slice(LEAGUE_CHAT_CHANNEL_PREFIX.length);
  }
  return null;
}

export interface RedisChatEnvelope {
  originInstanceId: string;
  message: ChatMessageDto;
  timestamp: number;
}

export interface IRedisPubSubClient {
  publish(channel: string, message: string): Promise<number>;
  subscribe(...channels: string[]): Promise<unknown>;
  unsubscribe(...channels: string[]): Promise<unknown>;
  on(event: "message", listener: (channel: string, message: string) => void): any;
  on(event: string, listener: (...args: any[]) => void): any;
  quit?(): Promise<unknown>;
  disconnect?(): void;
  status?: string;
}

export interface RedisChatAdapterOptions {
  instanceId?: string;
  publisher?: IRedisPubSubClient | null;
  subscriber?: IRedisPubSubClient | null;
  enabled?: boolean;
}

export class RedisChatAdapter implements ChatPubSubAdapter {
  public readonly instanceId: string;
  private publisher: IRedisPubSubClient | null = null;
  private subscriber: IRedisPubSubClient | null = null;
  private hub: ChatHub | null = null;
  private readonly subscribedChannels = new Set<string>();
  private isSubscribedToEvents = false;
  private isClosed = false;

  constructor(options: RedisChatAdapterOptions = {}) {
    this.instanceId = options.instanceId ?? `inst-${randomUUID()}`;

    const config = getRedisConfig();
    const enabled = options.enabled !== undefined ? options.enabled : config.enabled;

    if (enabled) {
      this.publisher = options.publisher !== undefined ? options.publisher : createRedisClient();
      this.subscriber = options.subscriber !== undefined ? options.subscriber : createRedisClient();
    }
  }

  public attach(hub: ChatHub): void {
    this.hub = hub;
    hub.setAdapter(this);
    this.setupSubscriber();
  }

  private setupSubscriber(): void {
    if (!this.subscriber || this.isSubscribedToEvents) return;

    this.subscriber.on("message", (channel: string, payload: string) => {
      this.handleIncomingMessage(channel, payload);
    });

    if (typeof (this.subscriber as any).on === "function") {
      this.subscriber.on("error", (err: Error) => {
        console.warn(`[RedisChatAdapter] Subscriber error on ${this.instanceId}:`, err.message);
      });
    }

    if (this.publisher && typeof (this.publisher as any).on === "function") {
      this.publisher.on("error", (err: Error) => {
        console.warn(`[RedisChatAdapter] Publisher error on ${this.instanceId}:`, err.message);
      });
    }

    this.isSubscribedToEvents = true;
  }

  public handleIncomingMessage(channel: string, payload: string): void {
    if (this.isClosed) return;

    const leagueId = parseLeagueIdFromChannel(channel);
    if (!leagueId) return;

    try {
      const parsed = JSON.parse(payload);
      const envelope: RedisChatEnvelope =
        parsed && typeof parsed === "object" && "originInstanceId" in parsed
          ? parsed
          : { originInstanceId: "unknown", message: parsed, timestamp: Date.now() };

      // Prevent echo to sockets connected to this instance (already delivered locally)
      if (envelope.originInstanceId === this.instanceId) {
        return;
      }

      if (this.hub) {
        this.hub.broadcastLocal(envelope.message);
      }
    } catch (error) {
      console.warn(`[RedisChatAdapter] Failed to parse incoming message on ${channel}:`, error);
    }
  }

  public async publish(message: ChatMessageDto): Promise<void> {
    if (this.isClosed || !this.publisher) return;

    const channel = getLeagueChatChannel(message.leagueId);
    const envelope: RedisChatEnvelope = {
      originInstanceId: this.instanceId,
      message,
      timestamp: Date.now(),
    };

    try {
      await this.publisher.publish(channel, JSON.stringify(envelope));
    } catch (error) {
      // Graceful degradation: local clients already received via ChatHub.broadcastLocal
      console.warn(`[RedisChatAdapter] Publish to Redis failed for league ${message.leagueId}:`, error);
    }
  }

  public async subscribeLeague(leagueId: string): Promise<void> {
    if (this.isClosed || !this.subscriber) return;

    const channel = getLeagueChatChannel(leagueId);
    if (this.subscribedChannels.has(channel)) return;

    this.subscribedChannels.add(channel);
    try {
      await this.subscriber.subscribe(channel);
    } catch (error) {
      console.warn(`[RedisChatAdapter] Failed to subscribe to channel ${channel}:`, error);
    }
  }

  public async unsubscribeLeague(leagueId: string): Promise<void> {
    if (this.isClosed || !this.subscriber) return;

    const channel = getLeagueChatChannel(leagueId);
    if (!this.subscribedChannels.has(channel)) return;

    this.subscribedChannels.delete(channel);
    try {
      await this.subscriber.unsubscribe(channel);
    } catch (error) {
      console.warn(`[RedisChatAdapter] Failed to unsubscribe from channel ${channel}:`, error);
    }
  }

  public getSubscribedChannels(): string[] {
    return Array.from(this.subscribedChannels);
  }

  public async close(): Promise<void> {
    this.isClosed = true;

    if (this.subscriber) {
      try {
        if (this.subscribedChannels.size > 0) {
          await this.subscriber.unsubscribe(...Array.from(this.subscribedChannels));
        }
      } catch {}
      try {
        if (typeof this.subscriber.quit === "function") {
          await this.subscriber.quit();
        } else if (typeof this.subscriber.disconnect === "function") {
          this.subscriber.disconnect();
        }
      } catch {}
    }

    if (this.publisher) {
      try {
        if (typeof this.publisher.quit === "function") {
          await this.publisher.quit();
        } else if (typeof this.publisher.disconnect === "function") {
          this.publisher.disconnect();
        }
      } catch {}
    }

    this.subscribedChannels.clear();
    if (this.hub && this.hub.getAdapter() === this) {
      this.hub.setAdapter(null);
    }
  }
}
