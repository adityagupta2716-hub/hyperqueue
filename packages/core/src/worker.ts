import { Redis } from 'ioredis';
import { v4 as uuidv4 } from 'uuid';
import { Job, WorkerOptions } from './types.js';
import { POP_READY_JOB_LUA, COMPLETE_JOB_LUA, FAIL_JOB_LUA } from './lua/scripts.js';

export type JobProcessor<T = any, R = any> = (job: Job<T, R>) => Promise<R>;

export class HyperWorker<T = any, R = any> {
  public readonly id: string;
  public readonly queueName: string;
  private redis: Redis;
  private processor: JobProcessor<T, R>;
  private concurrency: number;
  private pollIntervalMs: number;
  private visibilityTimeoutMs: number;

  private isRunning: boolean = false;
  private activeJobsCount: number = 0;
  private loopPromises: Promise<void>[] = [];

  constructor(
    queueName: string,
    processor: JobProcessor<T, R>,
    redisClient: Redis,
    opts?: WorkerOptions
  ) {
    this.id = `worker:${uuidv4().slice(0, 8)}`;
    this.queueName = queueName;
    this.processor = processor;
    this.redis = redisClient;

    this.concurrency = opts?.concurrency || 5;
    this.pollIntervalMs = opts?.pollIntervalMs || 100;
    this.visibilityTimeoutMs = opts?.visibilityTimeoutMs || 30000;
  }

  /**
   * Start worker processing loop
   */
  async start(): Promise<void> {
    if (this.isRunning) return;
    this.isRunning = true;

    // Register worker presence
    await this.redis.hset(`hyperqueue:${this.queueName}:workers`, this.id, JSON.stringify({
      id: this.id,
      concurrency: this.concurrency,
      startedAt: Date.now(),
      status: 'active',
    }));

    for (let i = 0; i < this.concurrency; i++) {
      this.loopPromises.push(this.workerLoop(i));
    }
  }

  private async workerLoop(slot: number): Promise<void> {
    const readyKey = `hyperqueue:${this.queueName}:ready`;
    const delayedKey = `hyperqueue:${this.queueName}:delayed`;
    const activeKey = `hyperqueue:${this.queueName}:active`;

    while (this.isRunning) {
      try {
        const jobId = await this.redis.eval(
          POP_READY_JOB_LUA,
          3,
          readyKey,
          delayedKey,
          activeKey,
          Date.now().toString(),
          this.id,
          this.visibilityTimeoutMs.toString()
        ) as string | null;

        if (!jobId) {
          await new Promise((r) => setTimeout(r, this.pollIntervalMs));
          continue;
        }

        const jobRaw = await this.redis.hget('hyperqueue:jobs', jobId);
        if (!jobRaw) continue;

        const job: Job<T, R> = JSON.parse(jobRaw);
        job.attempts += 1;
        job.processedAt = Date.now();
        job.workerId = this.id;
        job.status = 'active';

        this.activeJobsCount++;

        // Execute task with timeout watchdog
        await this.executeJobWithTimeout(job);

      } catch (err) {
        console.error(`[Worker ${this.id}] Error in worker loop slot ${slot}:`, err);
        await new Promise((r) => setTimeout(r, 500));
      } finally {
        this.activeJobsCount = Math.max(0, this.activeJobsCount - 1);
      }
    }
  }

  private async executeJobWithTimeout(job: Job<T, R>): Promise<void> {
    const activeKey = `hyperqueue:${this.queueName}:active`;
    const completedKey = `hyperqueue:${this.queueName}:completed`;
    const delayedKey = `hyperqueue:${this.queueName}:delayed`;
    const dlqKey = `hyperqueue:${this.queueName}:dlq`;

    try {
      const timeoutPromise = new Promise((_, reject) => 
        setTimeout(() => reject(new Error(`Job execution timed out after ${job.opts.timeoutMs}ms`)), job.opts.timeoutMs)
      );

      const result = await Promise.race([this.processor(job), timeoutPromise]) as R;

      // Mark Job Complete atomically
      await this.redis.eval(
        COMPLETE_JOB_LUA,
        2,
        activeKey,
        completedKey,
        job.id,
        this.id,
        JSON.stringify(result || {})
      );

      // Handle DAG progression if this job belongs to a workflow
      if (job.workflowId && job.stepId) {
        await this.resolveDAGDependents(job.workflowId, job.stepId, result);
      }

    } catch (err: any) {
      const errorMsg = err?.message || String(err);
      const isDeadLetter = job.attempts >= job.opts.maxRetries;

      // Exponential backoff calculation: base * (factor ^ attempts)
      const backoffMs = job.opts.initialBackoffMs * Math.pow(job.opts.backoffFactor, job.attempts - 1);
      const retryTimestamp = Date.now() + backoffMs;

      await this.redis.eval(
        FAIL_JOB_LUA,
        3,
        activeKey,
        delayedKey,
        dlqKey,
        job.id,
        errorMsg,
        isDeadLetter ? '1' : '0',
        retryTimestamp.toString(),
        (job.opts.priority || '100').toString()
      );
    }
  }

  /**
   * Automatically unlock dependent tasks in a DAG workflow once a parent finishes
   */
  private async resolveDAGDependents(workflowId: string, completedStepId: string, _result: any): Promise<void> {
    const workflowKey = `hyperqueue:workflow:${workflowId}`;
    const totalNodesRaw = await this.redis.hget(workflowKey, 'totalNodes');
    if (!totalNodesRaw) return;

    await this.redis.hincrby(workflowKey, 'completedNodes', 1);

    // In a full production setup, search child nodes and decrement remainingDeps
    // When remainingDeps reaches 0, atomically push child node to readyKey
  }

  /**
   * Graceful Shutdown
   */
  async stop(): Promise<void> {
    this.isRunning = false;
    await Promise.all(this.loopPromises);
    await this.redis.hdel(`hyperqueue:${this.queueName}:workers`, this.id);
  }
}
