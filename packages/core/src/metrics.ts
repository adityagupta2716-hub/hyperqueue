import client from 'prom-client';

// Initialize default Node.js telemetry metrics (CPU, Memory, Event loop lag)
client.collectDefaultMetrics({ prefix: 'hyperqueue_' });

export const metrics = {
  jobsProcessed: new client.Counter({
    name: 'hyperqueue_jobs_processed_total',
    help: 'Total number of background jobs processed by HyperQueue',
    labelNames: ['queue', 'status'],
  }),

  jobExecutionDuration: new client.Histogram({
    name: 'hyperqueue_job_execution_duration_seconds',
    help: 'Latency distribution of job execution in seconds',
    labelNames: ['queue', 'job_name'],
    buckets: [0.01, 0.05, 0.1, 0.5, 1, 2, 5, 10, 30],
  }),

  activeWorkers: new client.Gauge({
    name: 'hyperqueue_active_workers',
    help: 'Current count of connected active worker nodes',
    labelNames: ['queue'],
  }),

  queueDepth: new client.Gauge({
    name: 'hyperqueue_queue_depth',
    help: 'Number of jobs in each queue state',
    labelNames: ['queue', 'state'], // 'ready', 'delayed', 'active', 'dlq'
  }),

  register: client.register,
};
