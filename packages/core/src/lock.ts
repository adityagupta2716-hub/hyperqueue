import { Redis } from 'ioredis';
import { v4 as uuidv4 } from 'uuid';
import { ACQUIRE_LOCK_LUA, RELEASE_LOCK_LUA, EXTEND_LOCK_LUA } from './lua/scripts.js';
import { DistributedLockOptions } from './types.js';

export class DistributedLock {
  private redis: Redis;
  private key: string;
  private token: string;
  private ttlMs: number;
  private heartbeatTimer?: NodeJS.Timeout;

  constructor(redis: Redis, resourceName: string, opts?: DistributedLockOptions) {
    this.redis = redis;
    this.key = `hyperqueue:locks:${resourceName}`;
    this.token = uuidv4();
    this.ttlMs = opts?.ttlMs || 10000;
  }

  /**
   * Attempt to acquire the distributed lock.
   */
  async acquire(maxRetries = 5, retryDelayMs = 200): Promise<boolean> {
    for (let attempt = 0; attempt < maxRetries; attempt++) {
      const result = await this.redis.eval(
        ACQUIRE_LOCK_LUA,
        1,
        this.key,
        this.token,
        this.ttlMs.toString()
      );

      if (result === 1) {
        this.startHeartbeat();
        return true;
      }

      // Exponential jitter to avoid Thundering Herd problem
      const jitter = Math.random() * 50;
      await new Promise((res) => setTimeout(res, retryDelayMs + jitter));
    }
    return false;
  }

  /**
   * Safely extend the lock lease while background work is executing.
   */
  async extend(additionalTtlMs: number = this.ttlMs): Promise<boolean> {
    const result = await this.redis.eval(
      EXTEND_LOCK_LUA,
      1,
      this.key,
      this.token,
      additionalTtlMs.toString()
    );
    return result === 1;
  }

  /**
   * Safely release the lock (only if token still matches).
   */
  async release(): Promise<boolean> {
    this.stopHeartbeat();
    const result = await this.redis.eval(
      RELEASE_LOCK_LUA,
      1,
      this.key,
      this.token
    );
    return result === 1;
  }

  private startHeartbeat(): void {
    const interval = Math.max(1000, Math.floor(this.ttlMs / 3));
    this.heartbeatTimer = setInterval(async () => {
      try {
        const extended = await this.extend();
        if (!extended) {
          this.stopHeartbeat();
        }
      } catch {
        this.stopHeartbeat();
      }
    }, interval);
  }

  private stopHeartbeat(): void {
    if (this.heartbeatTimer) {
      clearInterval(this.heartbeatTimer);
      this.heartbeatTimer = undefined;
    }
  }

  public getToken(): string {
    return this.token;
  }
}
