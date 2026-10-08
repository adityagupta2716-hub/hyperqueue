import express, { Request, Response } from 'express';
import http from 'http';
import { WebSocketServer, WebSocket } from 'ws';
import cors from 'cors';
import { Redis } from 'ioredis';
import { HyperQueue, HyperWorker, metrics, DAGWorkflowEngine } from '@hyperqueue/core';

const app = express();
const server = http.createServer(app);
const wss = new WebSocketServer({ server, path: '/ws' });

app.use(cors());
app.use(express.json());

const REDIS_URL = process.env.REDIS_URL || 'redis://localhost:6379';
const redis = new Redis(REDIS_URL, { maxRetriesPerRequest: null });
const subRedis = new Redis(REDIS_URL, { maxRetriesPerRequest: null });

const defaultQueue = new HyperQueue('default', redis);

// --- WebSocket Live Broadcast ---
const clients = new Set<WebSocket>();

wss.on('connection', (ws) => {
  clients.add(ws);
  ws.on('close', () => clients.delete(ws));
});

// Subscribe to Redis event streams and broadcast live to UI
subRedis.psubscribe('hyperqueue:*:events', (err) => {
  if (err) console.error('Redis subscription error:', err);
});

subRedis.on('pmessage', (_pattern, channel, message) => {
  const payload = JSON.stringify({ channel, data: JSON.parse(message) });
  for (const client of clients) {
    if (client.readyState === WebSocket.OPEN) {
      client.send(payload);
    }
  }
});

// --- REST Endpoints ---

// 1. Prometheus Telemetry Endpoint
app.get('/metrics', async (_req: Request, res: Response) => {
  res.set('Content-Type', metrics.register.contentType);
  res.send(await metrics.register.metrics());
});

// 2. Health Check
app.get('/health', (_req: Request, res: Response) => {
  res.json({ status: 'ok', timestamp: Date.now() });
});

// 3. Queue Stats
app.get('/api/queues/:name/stats', async (req: Request, res: Response) => {
  const q = new HyperQueue(req.params.name, redis);
  const stats = await q.getStats();
  res.json({ queue: req.params.name, stats });
});

// 4. Submit Job
app.post('/api/jobs', async (req: Request, res: Response) => {
  const { name, data, priority, delayMs, deduplicationKey } = req.body;
  const queueName = req.body.queue || 'default';
  const q = new HyperQueue(queueName, redis);

  const job = await q.add(name, data, { priority, delayMs, deduplicationKey });
  res.status(201).json({ success: true, job });
});

// 5. Submit DAG Workflow
app.post('/api/workflows', async (req: Request, res: Response) => {
  const { name, nodes } = req.body;
  const engine = new DAGWorkflowEngine(defaultQueue);

  try {
    const result = await engine.submitWorkflow(name, nodes);
    res.status(201).json({ success: true, ...result });
  } catch (err: any) {
    res.status(400).json({ success: false, error: err.message });
  }
});

// --- Background Worker Initialization ---
const sampleWorker = new HyperWorker(
  'default',
  async (job) => {
    console.log(`[Worker] Processing job ${job.id} (${job.name})...`);
    // Simulate async job processing work
    await new Promise((res) => setTimeout(res, 500));
    return { processed: true, completedAt: Date.now() };
  },
  redis,
  { concurrency: 5 }
);

sampleWorker.start().then(() => {
  console.log('HyperQueue Worker Pool started successfully.');
});

const PORT = process.env.PORT || 4000;
server.listen(PORT, () => {
  console.log(`HyperQueue HTTP & WebSocket Server running on port ${PORT}`);
});
