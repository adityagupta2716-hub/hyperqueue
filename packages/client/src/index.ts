import { Redis } from 'ioredis';
import { HyperQueue, Job, JobOptions, DAGNode, DAGWorkflowEngine } from '@hyperqueue/core';

export interface HyperClientOptions {
  redisUrl?: string;
  defaultQueue?: string;
}

export class HyperClient {
  private redis: Redis;
  private defaultQueue: string;
  private queues: Map<string, HyperQueue> = new Map();

  constructor(opts?: HyperClientOptions) {
    this.redis = new Redis(opts?.redisUrl || process.env.REDIS_URL || 'redis://localhost:6379', {
      maxRetriesPerRequest: null,
      enableReadyCheck: false,
    });
    this.defaultQueue = opts?.defaultQueue || 'default';
  }

  getQueue(queueName: string = this.defaultQueue): HyperQueue {
    if (!this.queues.has(queueName)) {
      this.queues.set(queueName, new HyperQueue(queueName, this.redis));
    }
    return this.queues.get(queueName)!;
  }

  /**
   * Enqueue a single job
   */
  async enqueue<T = any, R = any>(
    name: string,
    data: T,
    opts?: JobOptions,
    queueName: string = this.defaultQueue
  ): Promise<Job<T, R>> {
    const queue = this.getQueue(queueName);
    return queue.add(name, data, opts);
  }

  /**
   * Submit a multi-step DAG workflow with automatic topological ordering
   */
  async submitWorkflow(workflowName: string, nodes: DAGNode[], queueName: string = this.defaultQueue) {
    const queue = this.getQueue(queueName);
    const engine = new DAGWorkflowEngine(queue);
    return engine.submitWorkflow(workflowName, nodes);
  }

  async close(): Promise<void> {
    await this.redis.quit();
  }
}
