import { Redis } from 'ioredis';
import { v4 as uuidv4 } from 'uuid';
import { Job, JobOptions, PriorityWeight, JobPriority } from './types.js';

export class HyperQueue<T = any, R = any> {
  public readonly name: string;
  public readonly redis: Redis;

  private readyKey: string;
  private delayedKey: string;
  private activeKey: string;
  private completedKey: string;
  private dlqKey: string;

  constructor(name: string, redisClient: Redis) {
    this.name = name;
    this.redis = redisClient;

    this.readyKey = `hyperqueue:${name}:ready`;
    this.delayedKey = `hyperqueue:${name}:delayed`;
    this.activeKey = `hyperqueue:${name}:active`;
    this.completedKey = `hyperqueue:${name}:completed`;
    this.dlqKey = `hyperqueue:${name}:dlq`;
  }

  /**
   * Enqueue a new job into the distributed priority queue.
   */
  async add(jobName: string, data: T, opts?: JobOptions): Promise<Job<T, R>> {
    const priority: JobPriority = opts?.priority || 'normal';
    const delayMs = opts?.delayMs || 0;
    const maxRetries = opts?.maxRetries ?? 3;
    const backoffFactor = opts?.backoffFactor ?? 2.0;
    const initialBackoffMs = opts?.initialBackoffMs ?? 1000;
    const timeoutMs = opts?.timeoutMs ?? 30000;

    // Idempotency / Deduplication check
    let jobId = uuidv4();
    if (opts?.deduplicationKey) {
      const dedupKey = `hyperqueue:${this.name}:dedup:${opts.deduplicationKey}`;
      const acquired = await this.redis.set(dedupKey, jobId, 'EX', 86400, 'NX'); // 24hr dedup window
      if (!acquired) {
        const existingJobId = await this.redis.get(dedupKey);
        if (existingJobId) {
          const existingJob = await this.getJob(existingJobId);
          if (existingJob) return existingJob;
        }
      }
    }

    const job: Job<T, R> = {
      id: jobId,
      name: jobName,
      data,
      opts: {
        priority,
        delayMs,
        maxRetries,
        backoffFactor,
        initialBackoffMs,
        timeoutMs,
        deduplicationKey: opts?.deduplicationKey || '',
      },
      status: delayMs > 0 ? 'delayed' : 'queued',
      attempts: 0,
      createdAt: Date.now(),
    };

    const pipeline = this.redis.pipeline();
    
    // Store job payload and priority
    pipeline.hset('hyperqueue:jobs', jobId, JSON.stringify(job));
    pipeline.hset('hyperqueue:job_priority', jobId, PriorityWeight[priority].toString());

    if (delayMs > 0) {
      const executeAt = Date.now() + delayMs;
      pipeline.zadd(this.delayedKey, executeAt, jobId);
    } else {
      pipeline.zadd(this.readyKey, PriorityWeight[priority], jobId);
    }

    // Publish event for live dashboards / listeners
    pipeline.publish(`hyperqueue:${this.name}:events`, JSON.stringify({
      event: 'job:created',
      jobId,
      jobName,
      priority,
      timestamp: Date.now(),
    }));

    await pipeline.exec();
    return job;
  }

  /**
   * Retrieve job by ID
   */
  async getJob(jobId: string): Promise<Job<T, R> | null> {
    const raw = await this.redis.hget('hyperqueue:jobs', jobId);
    if (!raw) return null;
    return JSON.parse(raw);
  }

  /**
   * Get queue statistics and health counts
   */
  async getStats(): Promise<{
    ready: number;
    delayed: number;
    active: number;
    completed: number;
    dlq: number;
  }> {
    const pipeline = this.redis.pipeline();
    pipeline.zcard(this.readyKey);
    pipeline.zcard(this.delayedKey);
    pipeline.zcard(this.activeKey);
    pipeline.scard(this.completedKey);
    pipeline.zcard(this.dlqKey);

    const results = await pipeline.exec();
    return {
      ready: (results?.[0]?.[1] as number) || 0,
      delayed: (results?.[1]?.[1] as number) || 0,
      active: (results?.[2]?.[1] as number) || 0,
      completed: (results?.[3]?.[1] as number) || 0,
      dlq: (results?.[4]?.[1] as number) || 0,
    };
  }
}
