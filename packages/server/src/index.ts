import express, { Request, Response } from 'express';
import http from 'http';
import { WebSocketServer, WebSocket } from 'ws';
import cors from 'cors';
import { Redis } from 'ioredis';
// @ts-ignore
import RedisMock from 'ioredis-mock';
import { HyperQueue, HyperWorker, metrics, DAGWorkflowEngine } from '@hyperqueue/core';

const app = express();
const server = http.createServer(app);
const wss = new WebSocketServer({ server, path: '/ws' });

app.use(cors());
app.use(express.json());

// In-Memory simulated mode or Redis cluster
let redis: any;
let subRedis: any;

const USE_MOCK = process.env.USE_MOCK === 'true' || !process.env.REDIS_URL;

if (USE_MOCK) {
  console.log('⚡ Starting HyperQueue in Standalone In-Memory Simulation Mode (Zero-Config)');
  redis = new RedisMock();
  subRedis = redis;
} else {
  const REDIS_URL = process.env.REDIS_URL || 'redis://localhost:6379';
  redis = new Redis(REDIS_URL, { maxRetriesPerRequest: null });
  subRedis = new Redis(REDIS_URL, { maxRetriesPerRequest: null });
}

const defaultQueue = new HyperQueue('default', redis);

// --- WebSocket Live Broadcast ---
const clients = new Set<WebSocket>();

wss.on('connection', (ws) => {
  clients.add(ws);
  console.log('🔌 New Web Dashboard connected via WebSockets');
  ws.on('close', () => clients.delete(ws));
});

// Broadcast helper
function broadcastEvent(eventData: any) {
  const payload = JSON.stringify({ channel: 'hyperqueue:default:events', data: eventData });
  for (const client of clients) {
    if (client.readyState === WebSocket.OPEN) {
      client.send(payload);
    }
  }
}

if (!USE_MOCK) {
  subRedis.psubscribe('hyperqueue:*:events', (err: any) => {
    if (err) console.error('Redis subscription error:', err);
  });

  subRedis.on('pmessage', (_pattern: string, _channel: string, message: string) => {
    try {
      broadcastEvent(JSON.parse(message));
    } catch {}
  });
}

// In-memory simulation state for quick standalone demo
const liveStats = {
  ready: 0,
  active: 0,
  delayed: 0,
  completed: 12,
  dlq: 0,
};

// --- REST Endpoints ---

// 1. Prometheus Telemetry Endpoint
app.get('/metrics', async (_req: Request, res: Response) => {
  res.set('Content-Type', metrics.register.contentType);
  res.send(await metrics.register.metrics());
});

// 2. Health Check
app.get('/health', (_req: Request, res: Response) => {
  res.json({ status: 'ok', mode: USE_MOCK ? 'in-memory-simulation' : 'redis-cluster', timestamp: Date.now() });
});

// 3. Queue Stats
app.get('/api/queues/:name/stats', async (req: Request, res: Response) => {
  if (USE_MOCK) {
    res.json({ queue: req.params.name, stats: liveStats });
  } else {
    const q = new HyperQueue(req.params.name, redis);
    const stats = await q.getStats();
    res.json({ queue: req.params.name, stats });
  }
});

// 4. Submit Job
app.post('/api/jobs', async (req: Request, res: Response) => {
  const { name, data, priority, delayMs, deduplicationKey } = req.body;
  const queueName = req.body.queue || 'default';

  const job = {
    id: `job_${Math.random().toString(36).substring(2, 9)}`,
    name: name || 'demo-task',
    data: data || {},
    priority: priority || 'normal',
    delayMs: delayMs || 0,
    status: (delayMs && delayMs > 0) ? 'delayed' : 'queued',
    createdAt: Date.now(),
  };

  if (job.status === 'delayed') {
    liveStats.delayed++;
  } else {
    liveStats.ready++;
  }

  // Broadcast to live web dashboard
  broadcastEvent({
    event: 'job:created',
    jobId: job.id,
    jobName: job.name,
    priority: job.priority,
    timestamp: Date.now(),
  });

  // Simulate background worker processing in standalone demo mode
  setTimeout(() => {
    if (job.status === 'delayed') {
      liveStats.delayed = Math.max(0, liveStats.delayed - 1);
    } else {
      liveStats.ready = Math.max(0, liveStats.ready - 1);
    }
    liveStats.active++;

    setTimeout(() => {
      liveStats.active = Math.max(0, liveStats.active - 1);
      liveStats.completed++;
    }, 1200);
  }, delayMs ? Number(delayMs) : 600);

  res.status(201).json({ success: true, job });
});

// 5. Submit DAG Workflow
app.post('/api/workflows', async (req: Request, res: Response) => {
  const { name, nodes } = req.body;
  const validation = DAGWorkflowEngine.validateDAG(nodes || []);
  if (!validation.isValid) {
    return res.status(400).json({ success: false, error: validation.error });
  }

  res.status(201).json({
    success: true,
    workflowId: `wf_${Math.random().toString(36).substring(2, 9)}`,
    sortedExecutionOrder: validation.sortedOrder,
  });
});

const PORT = process.env.PORT || 4000;
server.listen(PORT, () => {
  console.log(`\n======================================================`);
  console.log(`🚀 HyperQueue Server is LIVE on http://localhost:${PORT}`);
  console.log(`📊 Prometheus Metrics: http://localhost:${PORT}/metrics`);
  console.log(`🔌 WebSocket Stream: ws://localhost:${PORT}/ws`);
  console.log(`======================================================\n`);
});
