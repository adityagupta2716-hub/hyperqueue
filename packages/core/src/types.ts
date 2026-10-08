export type JobPriority = 'critical' | 'high' | 'normal' | 'low';

export const PriorityWeight: Record<JobPriority, number> = {
  critical: 1,
  high: 10,
  normal: 100,
  low: 1000,
};

export type JobStatus = 
  | 'queued' 
  | 'delayed' 
  | 'active' 
  | 'completed' 
  | 'failed' 
  | 'dead_letter';

export interface JobOptions {
  priority?: JobPriority;
  delayMs?: number;
  maxRetries?: number;
  backoffFactor?: number; // Exponential backoff multiplier (e.g. 2.0)
  initialBackoffMs?: number;
  timeoutMs?: number;
  deduplicationKey?: string; // Idempotency key
}

export interface Job<T = any, R = any> {
  id: string;
  name: string;
  data: T;
  opts: Required<JobOptions>;
  status: JobStatus;
  attempts: number;
  createdAt: number;
  processedAt?: number;
  finishedAt?: number;
  result?: R;
  error?: string;
  workerId?: string;
  workflowId?: string;
  stepId?: string;
}

export interface DAGNode<T = any> {
  id: string;
  name: string;
  data: T;
  dependencies: string[]; // IDs of preceding nodes that must complete first
  opts?: JobOptions;
}

export interface DAGWorkflow {
  id: string;
  name: string;
  nodes: Map<string, DAGNode>;
  createdAt: number;
  status: 'pending' | 'running' | 'completed' | 'failed';
}

export interface WorkerOptions {
  concurrency?: number;
  pollIntervalMs?: number;
  visibilityTimeoutMs?: number; // Lock duration before worker is considered dead
  heartbeatIntervalMs?: number;
}

export interface DistributedLockOptions {
  ttlMs?: number;
  retryIntervalMs?: number;
  maxRetryAttempts?: number;
}
