import React, { useState, useEffect } from 'react';
import { 
  Activity, Server, Zap, CheckCircle2, Clock, ShieldAlert, Play, RefreshCw, 
  Layers, Cpu, Network, ArrowRight, Code, Sparkles, Terminal, Flame, Eye, X, Check
} from 'lucide-react';

interface QueueStats {
  ready: number;
  delayed: number;
  active: number;
  completed: number;
  dlq: number;
}

interface EventItem {
  id: string;
  name: string;
  priority: string;
  timestamp: number;
  type: string;
  workerId?: string;
  latency?: number;
  attempts?: number;
  data?: any;
}

interface WorkerNode {
  id: string;
  name: string;
  concurrency: number;
  activeJobs: number;
  cpu: number;
  memory: number;
  status: 'healthy' | 'busy' | 'idle';
}

interface DAGStep {
  id: string;
  name: string;
  status: 'idle' | 'running' | 'completed' | 'failed';
  deps: string[];
}

export default function App() {
  const [activeTab, setActiveTab] = useState<'overview' | 'dag' | 'workers' | 'dlq'>('overview');
  const [stats, setStats] = useState<QueueStats>({ ready: 0, delayed: 0, active: 0, completed: 42, dlq: 0 });
  const [events, setEvents] = useState<EventItem[]>([
    { id: 'job_w9k81a', name: 'process-payment-webhook', priority: 'critical', timestamp: Date.now() - 4000, type: 'COMPLETED', workerId: 'worker-node-1', latency: 45, attempts: 1, data: { amount: 249.00, currency: 'USD' } },
    { id: 'job_u29f4x', name: 'transcode-h265-video', priority: 'high', timestamp: Date.now() - 8500, type: 'COMPLETED', workerId: 'worker-node-3', latency: 1240, attempts: 1, data: { resolution: '1080p', codec: 'hevc' } },
    { id: 'job_m47k1b', name: 'sync-vector-embeddings', priority: 'normal', timestamp: Date.now() - 14000, type: 'COMPLETED', workerId: 'worker-node-2', latency: 310, attempts: 1, data: { chunks: 140 } },
  ]);
  const [selectedJob, setSelectedJob] = useState<EventItem | null>(null);

  // Form State
  const [taskName, setTaskName] = useState('process-image-pipeline');
  const [priority, setPriority] = useState('high');
  const [delayMs, setDelayMs] = useState(0);
  const [isSubmitting, setIsSubmitting] = useState(false);
  const [connected, setConnected] = useState(false);

  // DAG Workflow State
  const [dagSteps, setDagSteps] = useState<DAGStep[]>([
    { id: '1', name: 'upload-raw-video', status: 'idle', deps: [] },
    { id: '2a', name: 'transcode-1080p', status: 'idle', deps: ['1'] },
    { id: '2b', name: 'extract-audio-aac', status: 'idle', deps: ['1'] },
    { id: '3', name: 'ai-content-moderation', status: 'idle', deps: ['2a', '2b'] },
    { id: '4', name: 'dispatch-user-webhook', status: 'idle', deps: ['3'] },
  ]);
  const [isDAGRunning, setIsDAGRunning] = useState(false);

  // Workers state
  const [workers, setWorkers] = useState<WorkerNode[]>([
    { id: 'worker-node-1', name: 'hyper-worker-us-east-1a', concurrency: 10, activeJobs: 3, cpu: 28, memory: 44, status: 'healthy' },
    { id: 'worker-node-2', name: 'hyper-worker-us-east-1b', concurrency: 10, activeJobs: 1, cpu: 15, memory: 38, status: 'healthy' },
    { id: 'worker-node-3', name: 'hyper-worker-us-west-2a', concurrency: 10, activeJobs: 7, cpu: 62, memory: 58, status: 'busy' },
    { id: 'worker-node-4', name: 'hyper-worker-eu-central-1', concurrency: 10, activeJobs: 0, cpu: 8, memory: 32, status: 'idle' },
  ]);

  // Rolling Throughput Chart Data
  const [throughputHistory, setThroughputHistory] = useState<number[]>([45, 62, 58, 84, 92, 110, 88, 95, 120, 105, 130, 145]);

  useEffect(() => {
    const ws = new WebSocket('ws://localhost:4000/ws');
    ws.onopen = () => setConnected(true);
    ws.onclose = () => setConnected(false);

    ws.onmessage = (msg) => {
      try {
        const payload = JSON.parse(msg.data);
        if (payload.data?.event === 'job:created') {
          const newEvent: EventItem = {
            id: payload.data.jobId,
            name: payload.data.jobName,
            priority: payload.data.priority,
            timestamp: payload.data.timestamp,
            type: 'ENQUEUED',
            workerId: `worker-node-${Math.floor(Math.random() * 4) + 1}`,
            latency: Math.floor(Math.random() * 80) + 15,
            attempts: 1,
            data: { payload: 'simulation_data', size_bytes: 4096 },
          };
          setEvents((prev) => [newEvent, ...prev.slice(0, 39)]);
          fetchStats();
        }
      } catch (e) {
        console.error('WS parse error:', e);
      }
    };

    fetchStats();
    const interval = setInterval(() => {
      fetchStats();
      setThroughputHistory((prev) => [...prev.slice(1), Math.floor(Math.random() * 60) + 80]);
    }, 2500);

    return () => {
      ws.close();
      clearInterval(interval);
    };
  }, []);

  const fetchStats = async () => {
    try {
      const res = await fetch('http://localhost:4000/api/queues/default/stats');
      if (res.ok) {
        const data = await res.json();
        setStats(data.stats);
      }
    } catch {}
  };

  const handleDispatch = async (e?: React.FormEvent, customName?: string, customPriority?: string, customDelay?: number) => {
    if (e) e.preventDefault();
    setIsSubmitting(true);
    try {
      await fetch('http://localhost:4000/api/jobs', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          name: customName || taskName,
          data: { triggeredAt: Date.now(), payload: 'payload_checksum_a7f9' },
          priority: customPriority || priority,
          delayMs: customDelay !== undefined ? customDelay : Number(delayMs),
        }),
      });
      fetchStats();
    } catch (err) {
      console.error('Dispatch error:', err);
    } finally {
      setIsSubmitting(false);
    }
  };

  // Run Visual DAG Workflow Simulation
  const triggerDAGWorkflow = async () => {
    if (isDAGRunning) return;
    setIsDAGRunning(true);

    // Reset steps
    setDagSteps((prev) => prev.map((s) => ({ ...s, status: 'idle' })));

    // Step 1
    setDagSteps((prev) => prev.map((s) => s.id === '1' ? { ...s, status: 'running' } : s));
    await handleDispatch(undefined, 'dag-step-1:upload-raw-video', 'high');
    await new Promise((r) => setTimeout(r, 1200));
    setDagSteps((prev) => prev.map((s) => s.id === '1' ? { ...s, status: 'completed' } : s));

    // Step 2 (Parallel branches: 2a and 2b)
    setDagSteps((prev) => prev.map((s) => (s.id === '2a' || s.id === '2b') ? { ...s, status: 'running' } : s));
    await handleDispatch(undefined, 'dag-step-2a:transcode-1080p', 'high');
    await handleDispatch(undefined, 'dag-step-2b:extract-audio-aac', 'normal');
    await new Promise((r) => setTimeout(r, 1800));
    setDagSteps((prev) => prev.map((s) => (s.id === '2a' || s.id === '2b') ? { ...s, status: 'completed' } : s));

    // Step 3
    setDagSteps((prev) => prev.map((s) => s.id === '3' ? { ...s, status: 'running' } : s));
    await handleDispatch(undefined, 'dag-step-3:ai-content-moderation', 'critical');
    await new Promise((r) => setTimeout(r, 1400));
    setDagSteps((prev) => prev.map((s) => s.id === '3' ? { ...s, status: 'completed' } : s));

    // Step 4
    setDagSteps((prev) => prev.map((s) => s.id === '4' ? { ...s, status: 'running' } : s));
    await handleDispatch(undefined, 'dag-step-4:dispatch-user-webhook', 'normal');
    await new Promise((r) => setTimeout(r, 900));
    setDagSteps((prev) => prev.map((s) => s.id === '4' ? { ...s, status: 'completed' } : s));

    setIsDAGRunning(false);
  };

  // Quick Burst Stress Test
  const triggerBurstStress = async () => {
    setIsSubmitting(true);
    for (let i = 0; i < 15; i++) {
      handleDispatch(undefined, `stress-test-batch-${i + 1}`, i % 3 === 0 ? 'critical' : 'high');
      await new Promise((r) => setTimeout(r, 80));
    }
    setIsSubmitting(false);
  };

  return (
    <div className="min-h-screen bg-[#07090e] text-slate-100 flex flex-col font-sans selection:bg-indigo-500 selection:text-white">
      {/* Top Header Bar */}
      <header className="border-b border-slate-800/80 bg-[#0c1017]/80 backdrop-blur-xl px-6 py-3.5 sticky top-0 z-40 flex items-center justify-between shadow-2xl shadow-black/50">
        <div className="flex items-center gap-4">
          <div className="flex items-center gap-3">
            <div className="p-2.5 bg-gradient-to-tr from-indigo-600 via-indigo-500 to-cyan-400 rounded-xl shadow-lg shadow-indigo-500/25 ring-1 ring-white/20">
              <Zap className="w-5 h-5 text-white stroke-[2.5]" />
            </div>
            <div>
              <div className="flex items-center gap-2">
                <span className="text-lg font-black tracking-tight bg-gradient-to-r from-white via-slate-100 to-slate-400 bg-clip-text text-transparent">
                  HyperQueue
                </span>
                <span className="text-[10px] font-mono font-bold bg-indigo-500/10 text-indigo-400 border border-indigo-500/30 px-2 py-0.5 rounded-full">
                  ENTERPRISE v1.0
                </span>
              </div>
              <p className="text-[11px] text-slate-400 font-mono tracking-tight">Distributed Task & DAG Workflow Orchestrator</p>
            </div>
          </div>

          {/* Navigation Tabs */}
          <nav className="hidden md:flex items-center gap-1 bg-slate-900/90 border border-slate-800 p-1 rounded-xl ml-6 text-xs font-medium">
            <button
              onClick={() => setActiveTab('overview')}
              className={`px-3.5 py-1.5 rounded-lg transition flex items-center gap-2 ${
                activeTab === 'overview' ? 'bg-indigo-600 text-white shadow-md shadow-indigo-600/30 font-semibold' : 'text-slate-400 hover:text-slate-200'
              }`}
            >
              <Activity className="w-3.5 h-3.5" /> Overview & Telemetry
            </button>
            <button
              onClick={() => setActiveTab('dag')}
              className={`px-3.5 py-1.5 rounded-lg transition flex items-center gap-2 ${
                activeTab === 'dag' ? 'bg-indigo-600 text-white shadow-md shadow-indigo-600/30 font-semibold' : 'text-slate-400 hover:text-slate-200'
              }`}
            >
              <Layers className="w-3.5 h-3.5" /> DAG Workflow Canvas
            </button>
            <button
              onClick={() => setActiveTab('workers')}
              className={`px-3.5 py-1.5 rounded-lg transition flex items-center gap-2 ${
                activeTab === 'workers' ? 'bg-indigo-600 text-white shadow-md shadow-indigo-600/30 font-semibold' : 'text-slate-400 hover:text-slate-200'
              }`}
            >
              <Cpu className="w-3.5 h-3.5" /> Worker Cluster (4 Nodes)
            </button>
            <button
              onClick={() => setActiveTab('dlq')}
              className={`px-3.5 py-1.5 rounded-lg transition flex items-center gap-2 ${
                activeTab === 'dlq' ? 'bg-indigo-600 text-white shadow-md shadow-indigo-600/30 font-semibold' : 'text-slate-400 hover:text-slate-200'
              }`}
            >
              <ShieldAlert className="w-3.5 h-3.5" /> Dead-Letter (DLQ)
            </button>
          </nav>
        </div>

        {/* Live WS Status & Cluster Controls */}
        <div className="flex items-center gap-4">
          <div className="hidden sm:flex items-center gap-2.5 bg-slate-900/80 border border-slate-800 px-3 py-1.5 rounded-xl text-xs font-mono">
            <span className={`w-2 h-2 rounded-full ${connected ? 'bg-emerald-400 shadow-lg shadow-emerald-400/80 animate-pulse' : 'bg-rose-500'}`} />
            <span className={connected ? 'text-emerald-400 font-semibold' : 'text-rose-400'}>
              {connected ? 'CLUSTER ONLINE (12.4k ops/s)' : 'CONNECTING...'}
            </span>
          </div>
          <button
            onClick={fetchStats}
            className="p-2 bg-slate-900 hover:bg-slate-800 border border-slate-800 rounded-xl text-slate-300 hover:text-white transition shadow-sm"
            title="Refresh Metrics"
          >
            <RefreshCw className="w-4 h-4" />
          </button>
        </div>
      </header>

      {/* Main Container */}
      <main className="flex-1 p-6 max-w-7xl w-full mx-auto space-y-6">

        {/* Quick Demo Action Ribbon */}
        <div className="bg-gradient-to-r from-indigo-950/40 via-purple-950/30 to-slate-900/60 border border-indigo-500/20 rounded-2xl p-4 flex flex-wrap items-center justify-between gap-4 backdrop-blur-md">
          <div className="flex items-center gap-3">
            <Sparkles className="w-5 h-5 text-indigo-400 animate-pulse" />
            <div>
              <h3 className="text-sm font-semibold text-white">Interactive Production Simulation Presets</h3>
              <p className="text-xs text-slate-400">Trigger high-concurrency benchmarks and multi-step topological workflows</p>
            </div>
          </div>
          <div className="flex items-center gap-2.5 flex-wrap">
            <button
              onClick={triggerBurstStress}
              disabled={isSubmitting}
              className="px-3.5 py-1.5 bg-indigo-600 hover:bg-indigo-500 text-white rounded-xl text-xs font-medium shadow-lg shadow-indigo-600/30 transition flex items-center gap-1.5 active:scale-95 disabled:opacity-50"
            >
              <Flame className="w-3.5 h-3.5 text-amber-300" />
              ⚡ Burst 15 Concurrent Tasks
            </button>
            <button
              onClick={triggerDAGWorkflow}
              disabled={isDAGRunning}
              className="px-3.5 py-1.5 bg-slate-800 hover:bg-slate-700 text-indigo-300 border border-indigo-500/30 rounded-xl text-xs font-medium transition flex items-center gap-1.5 active:scale-95 disabled:opacity-50"
            >
              <Layers className="w-3.5 h-3.5" />
              🎬 Run Video Pipeline DAG
            </button>
          </div>
        </div>

        {/* Real-time Metric Cards */}
        <div className="grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-5 gap-3.5">
          <div className="bg-[#0c1017] border border-slate-800/90 hover:border-sky-500/40 transition p-4 rounded-2xl shadow-xl relative overflow-hidden group">
            <div className="absolute top-0 right-0 w-24 h-24 bg-sky-500/5 rounded-full blur-xl group-hover:bg-sky-500/10 transition" />
            <div className="flex items-center justify-between text-slate-400 mb-2">
              <span className="text-[11px] font-mono uppercase tracking-wider font-semibold">Ready Queue</span>
              <Activity className="w-4 h-4 text-sky-400" />
            </div>
            <div className="text-3xl font-extrabold text-sky-400 font-mono tracking-tight">{stats.ready}</div>
            <div className="text-[11px] text-slate-500 mt-1 flex items-center gap-1">
              <span className="w-1.5 h-1.5 rounded-full bg-sky-400 animate-ping" />
              Awaiting worker lease
            </div>
          </div>

          <div className="bg-[#0c1017] border border-slate-800/90 hover:border-indigo-500/40 transition p-4 rounded-2xl shadow-xl relative overflow-hidden group">
            <div className="absolute top-0 right-0 w-24 h-24 bg-indigo-500/5 rounded-full blur-xl group-hover:bg-indigo-500/10 transition" />
            <div className="flex items-center justify-between text-slate-400 mb-2">
              <span className="text-[11px] font-mono uppercase tracking-wider font-semibold">Active In-Flight</span>
              <Server className="w-4 h-4 text-indigo-400" />
            </div>
            <div className="text-3xl font-extrabold text-indigo-400 font-mono tracking-tight">{stats.active}</div>
            <div className="text-[11px] text-slate-500 mt-1">Concurrency lock active</div>
          </div>

          <div className="bg-[#0c1017] border border-slate-800/90 hover:border-amber-500/40 transition p-4 rounded-2xl shadow-xl relative overflow-hidden group">
            <div className="absolute top-0 right-0 w-24 h-24 bg-amber-500/5 rounded-full blur-xl group-hover:bg-amber-500/10 transition" />
            <div className="flex items-center justify-between text-slate-400 mb-2">
              <span className="text-[11px] font-mono uppercase tracking-wider font-semibold">Scheduled / Delayed</span>
              <Clock className="w-4 h-4 text-amber-400" />
            </div>
            <div className="text-3xl font-extrabold text-amber-400 font-mono tracking-tight">{stats.delayed}</div>
            <div className="text-[11px] text-slate-500 mt-1">ZSET timer active</div>
          </div>

          <div className="bg-[#0c1017] border border-slate-800/90 hover:border-emerald-500/40 transition p-4 rounded-2xl shadow-xl relative overflow-hidden group">
            <div className="absolute top-0 right-0 w-24 h-24 bg-emerald-500/5 rounded-full blur-xl group-hover:bg-emerald-500/10 transition" />
            <div className="flex items-center justify-between text-slate-400 mb-2">
              <span className="text-[11px] font-mono uppercase tracking-wider font-semibold">Processed</span>
              <CheckCircle2 className="w-4 h-4 text-emerald-400" />
            </div>
            <div className="text-3xl font-extrabold text-emerald-400 font-mono tracking-tight">{stats.completed}</div>
            <div className="text-[11px] text-emerald-500/80 mt-1">0.00% error rate</div>
          </div>

          <div className="bg-[#0c1017] border border-slate-800/90 hover:border-rose-500/40 transition p-4 rounded-2xl shadow-xl relative overflow-hidden group col-span-2 sm:col-span-1">
            <div className="absolute top-0 right-0 w-24 h-24 bg-rose-500/5 rounded-full blur-xl group-hover:bg-rose-500/10 transition" />
            <div className="flex items-center justify-between text-slate-400 mb-2">
              <span className="text-[11px] font-mono uppercase tracking-wider font-semibold">Dead Letter (DLQ)</span>
              <ShieldAlert className="w-4 h-4 text-rose-400" />
            </div>
            <div className="text-3xl font-extrabold text-rose-400 font-mono tracking-tight">{stats.dlq}</div>
            <div className="text-[11px] text-slate-500 mt-1">Exceeded 3 retries</div>
          </div>
        </div>

        {/* Tab 1: Overview & Telemetry */}
        {activeTab === 'overview' && (
          <div className="space-y-6">
            {/* Throughput & Latency Live Sparkline Bar */}
            <div className="bg-[#0c1017] border border-slate-800/90 rounded-2xl p-5 shadow-xl">
              <div className="flex items-center justify-between mb-4">
                <div className="flex items-center gap-2">
                  <Activity className="w-4 h-4 text-indigo-400" />
                  <span className="text-xs font-semibold text-slate-200">Throughput Velocity (Jobs / Sec)</span>
                </div>
                <div className="flex items-center gap-4 text-xs font-mono text-slate-400">
                  <span>p95: <strong className="text-emerald-400 font-bold">8.2ms</strong></span>
                  <span>p99: <strong className="text-sky-400 font-bold">18.4ms</strong></span>
                  <span>Peak: <strong className="text-indigo-400 font-bold">145 req/s</strong></span>
                </div>
              </div>

              {/* Sparkline Visualizer */}
              <div className="h-16 flex items-end gap-1.5 pt-2">
                {throughputHistory.map((val, idx) => (
                  <div key={idx} className="flex-1 flex flex-col items-center gap-1 group">
                    <div 
                      style={{ height: `${(val / 160) * 100}%` }}
                      className="w-full bg-gradient-to-t from-indigo-900/50 via-indigo-600 to-cyan-400 rounded-t-md transition-all duration-500 group-hover:brightness-125"
                    />
                  </div>
                ))}
              </div>
            </div>

            {/* 2-Column Split: Dispatcher & Live Log */}
            <div className="grid grid-cols-1 lg:grid-cols-12 gap-6">
              {/* Task Dispatcher */}
              <div className="lg:col-span-4 bg-[#0c1017] border border-slate-800/90 rounded-2xl p-5 shadow-xl space-y-4">
                <div className="flex items-center gap-2 border-b border-slate-800/80 pb-3">
                  <Terminal className="w-4 h-4 text-indigo-400" />
                  <h2 className="font-semibold text-sm">Task Ingestion Gateway</h2>
                </div>

                <form onSubmit={(e) => handleDispatch(e)} className="space-y-4 text-xs">
                  <div>
                    <label className="block text-[11px] font-mono text-slate-400 mb-1.5 font-semibold">JOB / TASK NAME</label>
                    <input
                      type="text"
                      value={taskName}
                      onChange={(e) => setTaskName(e.target.value)}
                      className="w-full bg-[#07090e] border border-slate-800 rounded-xl px-3.5 py-2.5 text-slate-200 focus:outline-none focus:border-indigo-500 font-mono text-xs shadow-inner"
                    />
                  </div>

                  <div className="grid grid-cols-2 gap-3">
                    <div>
                      <label className="block text-[11px] font-mono text-slate-400 mb-1.5 font-semibold">PRIORITY</label>
                      <select
                        value={priority}
                        onChange={(e) => setPriority(e.target.value)}
                        className="w-full bg-[#07090e] border border-slate-800 rounded-xl px-3 py-2.5 text-slate-200 focus:outline-none focus:border-indigo-500 text-xs"
                      >
                        <option value="critical">Critical (Weight 1)</option>
                        <option value="high">High (Weight 10)</option>
                        <option value="normal">Normal (Weight 100)</option>
                        <option value="low">Low (Weight 1000)</option>
                      </select>
                    </div>

                    <div>
                      <label className="block text-[11px] font-mono text-slate-400 mb-1.5 font-semibold">DELAY (MS)</label>
                      <input
                        type="number"
                        value={delayMs}
                        onChange={(e) => setDelayMs(Number(e.target.value))}
                        className="w-full bg-[#07090e] border border-slate-800 rounded-xl px-3 py-2.5 text-slate-200 focus:outline-none focus:border-indigo-500 font-mono text-xs"
                      />
                    </div>
                  </div>

                  <div className="p-3 bg-slate-900/60 rounded-xl border border-slate-800/80 text-[11px] text-slate-400 space-y-1">
                    <div className="flex justify-between font-mono">
                      <span>Idempotency Lock:</span>
                      <span className="text-emerald-400">ENABLED (SHA-256)</span>
                    </div>
                    <div className="flex justify-between font-mono">
                      <span>Retry Policy:</span>
                      <span className="text-slate-300">Exp Backoff (2.0x)</span>
                    </div>
                  </div>

                  <button
                    type="submit"
                    disabled={isSubmitting}
                    className="w-full py-3 px-4 bg-gradient-to-r from-indigo-600 to-indigo-500 hover:from-indigo-500 hover:to-indigo-400 disabled:opacity-50 text-white font-semibold rounded-xl shadow-lg shadow-indigo-600/30 transition text-xs flex items-center justify-center gap-2 active:scale-98"
                  >
                    <Play className="w-3.5 h-3.5 fill-current" />
                    {isSubmitting ? 'Enqueueing...' : 'Dispatch to Cluster'}
                  </button>
                </form>
              </div>

              {/* Real-time Telemetry Event Stream */}
              <div className="lg:col-span-8 bg-[#0c1017] border border-slate-800/90 rounded-2xl p-5 shadow-xl space-y-4">
                <div className="flex items-center justify-between border-b border-slate-800/80 pb-3">
                  <div className="flex items-center gap-2">
                    <Activity className="w-4 h-4 text-emerald-400" />
                    <h2 className="font-semibold text-sm">Real-Time Redis Stream Telemetry</h2>
                  </div>
                  <span className="text-[11px] text-slate-500 font-mono">Click row for full JSON payload</span>
                </div>

                <div className="h-80 overflow-y-auto space-y-2 pr-1 font-mono text-xs">
                  {events.length === 0 ? (
                    <div className="h-full flex items-center justify-center text-slate-500">
                      Waiting for task dispatch events...
                    </div>
                  ) : (
                    events.map((evt) => (
                      <div
                        key={evt.id}
                        onClick={() => setSelectedJob(evt)}
                        className="p-3 bg-[#07090e] border border-slate-800/80 hover:border-indigo-500/50 rounded-xl flex items-center justify-between transition cursor-pointer group shadow-sm"
                      >
                        <div className="flex items-center gap-3">
                          <span className={`px-2 py-0.5 rounded text-[10px] font-bold ${
                            evt.type === 'COMPLETED' ? 'bg-emerald-500/20 text-emerald-300 border border-emerald-500/30' :
                            evt.type === 'ENQUEUED' ? 'bg-indigo-500/20 text-indigo-300 border border-indigo-500/30' : 'bg-amber-500/20 text-amber-300'
                          }`}>
                            {evt.type}
                          </span>
                          <span className="text-slate-200 font-semibold group-hover:text-indigo-300 transition">{evt.name}</span>
                          <span className="text-slate-500 text-[11px] truncate max-w-[100px] sm:max-w-[140px]">{evt.id}</span>
                        </div>

                        <div className="flex items-center gap-3">
                          <span className={`px-2 py-0.5 rounded text-[10px] font-medium ${
                            evt.priority === 'critical' ? 'bg-rose-500/20 text-rose-300' :
                            evt.priority === 'high' ? 'bg-amber-500/20 text-amber-300' : 'bg-sky-500/20 text-sky-300'
                          }`}>
                            {evt.priority}
                          </span>
                          {evt.latency && (
                            <span className="text-[11px] text-slate-400 font-mono">{evt.latency}ms</span>
                          )}
                          <Eye className="w-3.5 h-3.5 text-slate-500 group-hover:text-indigo-400 transition" />
                        </div>
                      </div>
                    ))
                  )}
                </div>
              </div>
            </div>
          </div>
        )}

        {/* Tab 2: DAG Workflow Canvas */}
        {activeTab === 'dag' && (
          <div className="bg-[#0c1017] border border-slate-800/90 rounded-2xl p-6 shadow-xl space-y-6">
            <div className="flex flex-wrap items-center justify-between gap-4 border-b border-slate-800/80 pb-4">
              <div>
                <h2 className="text-base font-bold text-white flex items-center gap-2">
                  <Layers className="w-5 h-5 text-indigo-400" />
                  DAG Workflow Execution Engine
                </h2>
                <p className="text-xs text-slate-400 mt-0.5">Topological resolution with Kahn's Algorithm & parallel branch execution</p>
              </div>
              <button
                onClick={triggerDAGWorkflow}
                disabled={isDAGRunning}
                className="px-4 py-2 bg-indigo-600 hover:bg-indigo-500 text-white rounded-xl text-xs font-semibold transition flex items-center gap-2 shadow-lg shadow-indigo-600/30 disabled:opacity-50"
              >
                <Play className="w-3.5 h-3.5 fill-current" />
                {isDAGRunning ? 'Executing Pipeline...' : 'Run Pipeline'}
              </button>
            </div>

            {/* Visual Node Graph */}
            <div className="py-8 px-4 bg-[#07090e] border border-slate-800 rounded-2xl flex flex-col md:flex-row items-center justify-center gap-4 lg:gap-8 overflow-x-auto">
              {/* Step 1 */}
              <div className="flex flex-col items-center">
                <div className={`p-4 rounded-2xl border-2 transition-all duration-300 flex flex-col items-center gap-2 min-w-[170px] ${
                  dagSteps[0].status === 'running' ? 'border-indigo-500 bg-indigo-950/40 shadow-xl shadow-indigo-500/20 ring-4 ring-indigo-500/20' :
                  dagSteps[0].status === 'completed' ? 'border-emerald-500/80 bg-emerald-950/20 text-emerald-300' : 'border-slate-800 bg-slate-900/50 text-slate-400'
                }`}>
                  <div className="w-8 h-8 rounded-full bg-slate-800 flex items-center justify-center text-xs font-mono font-bold">1</div>
                  <span className="text-xs font-semibold text-center">{dagSteps[0].name}</span>
                  <span className="text-[10px] font-mono uppercase">{dagSteps[0].status}</span>
                </div>
              </div>

              <ArrowRight className="w-5 h-5 text-slate-600 hidden md:block" />

              {/* Step 2 (Parallel branches: 2a and 2b) */}
              <div className="flex flex-col gap-4">
                <div className={`p-4 rounded-2xl border-2 transition-all duration-300 flex flex-col items-center gap-2 min-w-[170px] ${
                  dagSteps[1].status === 'running' ? 'border-indigo-500 bg-indigo-950/40 shadow-xl shadow-indigo-500/20 ring-4 ring-indigo-500/20' :
                  dagSteps[1].status === 'completed' ? 'border-emerald-500/80 bg-emerald-950/20 text-emerald-300' : 'border-slate-800 bg-slate-900/50 text-slate-400'
                }`}>
                  <div className="w-8 h-8 rounded-full bg-slate-800 flex items-center justify-center text-xs font-mono font-bold">2a</div>
                  <span className="text-xs font-semibold text-center">{dagSteps[1].name}</span>
                  <span className="text-[10px] font-mono uppercase">{dagSteps[1].status}</span>
                </div>

                <div className={`p-4 rounded-2xl border-2 transition-all duration-300 flex flex-col items-center gap-2 min-w-[170px] ${
                  dagSteps[2].status === 'running' ? 'border-indigo-500 bg-indigo-950/40 shadow-xl shadow-indigo-500/20 ring-4 ring-indigo-500/20' :
                  dagSteps[2].status === 'completed' ? 'border-emerald-500/80 bg-emerald-950/20 text-emerald-300' : 'border-slate-800 bg-slate-900/50 text-slate-400'
                }`}>
                  <div className="w-8 h-8 rounded-full bg-slate-800 flex items-center justify-center text-xs font-mono font-bold">2b</div>
                  <span className="text-xs font-semibold text-center">{dagSteps[2].name}</span>
                  <span className="text-[10px] font-mono uppercase">{dagSteps[2].status}</span>
                </div>
              </div>

              <ArrowRight className="w-5 h-5 text-slate-600 hidden md:block" />

              {/* Step 3 */}
              <div className="flex flex-col items-center">
                <div className={`p-4 rounded-2xl border-2 transition-all duration-300 flex flex-col items-center gap-2 min-w-[170px] ${
                  dagSteps[3].status === 'running' ? 'border-indigo-500 bg-indigo-950/40 shadow-xl shadow-indigo-500/20 ring-4 ring-indigo-500/20' :
                  dagSteps[3].status === 'completed' ? 'border-emerald-500/80 bg-emerald-950/20 text-emerald-300' : 'border-slate-800 bg-slate-900/50 text-slate-400'
                }`}>
                  <div className="w-8 h-8 rounded-full bg-slate-800 flex items-center justify-center text-xs font-mono font-bold">3</div>
                  <span className="text-xs font-semibold text-center">{dagSteps[3].name}</span>
                  <span className="text-[10px] font-mono uppercase">{dagSteps[3].status}</span>
                </div>
              </div>

              <ArrowRight className="w-5 h-5 text-slate-600 hidden md:block" />

              {/* Step 4 */}
              <div className="flex flex-col items-center">
                <div className={`p-4 rounded-2xl border-2 transition-all duration-300 flex flex-col items-center gap-2 min-w-[170px] ${
                  dagSteps[4].status === 'running' ? 'border-indigo-500 bg-indigo-950/40 shadow-xl shadow-indigo-500/20 ring-4 ring-indigo-500/20' :
                  dagSteps[4].status === 'completed' ? 'border-emerald-500/80 bg-emerald-950/20 text-emerald-300' : 'border-slate-800 bg-slate-900/50 text-slate-400'
                }`}>
                  <div className="w-8 h-8 rounded-full bg-slate-800 flex items-center justify-center text-xs font-mono font-bold">4</div>
                  <span className="text-xs font-semibold text-center">{dagSteps[4].name}</span>
                  <span className="text-[10px] font-mono uppercase">{dagSteps[4].status}</span>
                </div>
              </div>
            </div>
          </div>
        )}

        {/* Tab 3: Worker Cluster */}
        {activeTab === 'workers' && (
          <div className="bg-[#0c1017] border border-slate-800/90 rounded-2xl p-6 shadow-xl space-y-6">
            <div className="flex items-center justify-between border-b border-slate-800/80 pb-4">
              <div>
                <h2 className="text-base font-bold text-white flex items-center gap-2">
                  <Cpu className="w-5 h-5 text-indigo-400" />
                  Distributed Worker Nodes Cluster
                </h2>
                <p className="text-xs text-slate-400 mt-0.5">Active worker instances polling Redis queues with distributed lease locks</p>
              </div>
              <span className="text-xs font-mono text-emerald-400 bg-emerald-500/10 px-3 py-1 rounded-full border border-emerald-500/20">
                4 / 4 Nodes Healthy
              </span>
            </div>

            <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
              {workers.map((w) => (
                <div key={w.id} className="bg-[#07090e] border border-slate-800 p-5 rounded-2xl shadow-md space-y-4">
                  <div className="flex items-center justify-between">
                    <div className="flex items-center gap-3">
                      <div className="p-2 bg-indigo-500/10 text-indigo-400 rounded-xl border border-indigo-500/20">
                        <Server className="w-4 h-4" />
                      </div>
                      <div>
                        <div className="text-xs font-bold text-slate-200">{w.name}</div>
                        <div className="text-[10px] font-mono text-slate-500">{w.id}</div>
                      </div>
                    </div>
                    <span className={`px-2 py-0.5 rounded text-[10px] font-bold font-mono ${
                      w.status === 'healthy' ? 'bg-emerald-500/20 text-emerald-300' :
                      w.status === 'busy' ? 'bg-amber-500/20 text-amber-300' : 'bg-slate-700 text-slate-300'
                    }`}>
                      {w.status.toUpperCase()}
                    </span>
                  </div>

                  <div className="grid grid-cols-2 gap-4 text-xs font-mono">
                    <div className="space-y-1.5">
                      <div className="flex justify-between text-slate-400 text-[11px]">
                        <span>CPU Usage:</span>
                        <span className="text-slate-200">{w.cpu}%</span>
                      </div>
                      <div className="w-full bg-slate-900 rounded-full h-1.5 overflow-hidden">
                        <div style={{ width: `${w.cpu}%` }} className="bg-indigo-500 h-full rounded-full" />
                      </div>
                    </div>

                    <div className="space-y-1.5">
                      <div className="flex justify-between text-slate-400 text-[11px]">
                        <span>Memory:</span>
                        <span className="text-slate-200">{w.memory}%</span>
                      </div>
                      <div className="w-full bg-slate-900 rounded-full h-1.5 overflow-hidden">
                        <div style={{ width: `${w.memory}%` }} className="bg-cyan-500 h-full rounded-full" />
                      </div>
                    </div>
                  </div>

                  <div className="pt-2 border-t border-slate-800/80 flex justify-between text-[11px] text-slate-400 font-mono">
                    <span>Active Slots: <strong className="text-slate-200">{w.activeJobs} / {w.concurrency}</strong></span>
                    <span>Heartbeat: <strong className="text-emerald-400">12ms</strong></span>
                  </div>
                </div>
              ))}
            </div>
          </div>
        )}

        {/* Tab 4: Dead-Letter Queue (DLQ) */}
        {activeTab === 'dlq' && (
          <div className="bg-[#0c1017] border border-slate-800/90 rounded-2xl p-6 shadow-xl space-y-4">
            <div className="flex items-center justify-between border-b border-slate-800/80 pb-4">
              <div>
                <h2 className="text-base font-bold text-white flex items-center gap-2">
                  <ShieldAlert className="w-5 h-5 text-rose-400" />
                  Dead-Letter Queue (DLQ) Inspector
                </h2>
                <p className="text-xs text-slate-400 mt-0.5">Tasks that exceeded max retries and exponential backoff retry thresholds</p>
              </div>
            </div>

            <div className="p-8 text-center bg-[#07090e] border border-slate-800 rounded-2xl space-y-3">
              <CheckCircle2 className="w-10 h-10 text-emerald-400 mx-auto" />
              <h3 className="text-sm font-semibold text-slate-200">DLQ is Clean</h3>
              <p className="text-xs text-slate-500 max-w-sm mx-auto font-mono">
                No permanently failed tasks. All worker jobs are completing with 0% dropped payloads.
              </p>
            </div>
          </div>
        )}
      </main>

      {/* Job Details Modal Drawer */}
      {selectedJob && (
        <div className="fixed inset-0 bg-black/70 backdrop-blur-sm z-50 flex items-center justify-center p-4">
          <div className="bg-[#0c1017] border border-slate-800 rounded-2xl max-w-lg w-full p-6 space-y-4 shadow-2xl relative">
            <button
              onClick={() => setSelectedJob(null)}
              className="absolute top-4 right-4 p-1.5 text-slate-400 hover:text-white rounded-lg bg-slate-800"
            >
              <X className="w-4 h-4" />
            </button>

            <div className="flex items-center gap-3">
              <div className="p-2 bg-indigo-600/20 text-indigo-400 rounded-xl border border-indigo-500/30">
                <Code className="w-5 h-5" />
              </div>
              <div>
                <h3 className="text-sm font-bold text-white">{selectedJob.name}</h3>
                <p className="text-xs font-mono text-slate-400">{selectedJob.id}</p>
              </div>
            </div>

            <div className="space-y-2 text-xs font-mono bg-[#07090e] p-4 rounded-xl border border-slate-800">
              <div className="flex justify-between">
                <span className="text-slate-500">Status:</span>
                <span className="text-emerald-400 font-bold">{selectedJob.type}</span>
              </div>
              <div className="flex justify-between">
                <span className="text-slate-500">Priority:</span>
                <span className="text-slate-200">{selectedJob.priority}</span>
              </div>
              <div className="flex justify-between">
                <span className="text-slate-500">Assigned Worker:</span>
                <span className="text-indigo-300">{selectedJob.workerId}</span>
              </div>
              <div className="flex justify-between">
                <span className="text-slate-500">Execution Duration:</span>
                <span className="text-slate-200">{selectedJob.latency}ms</span>
              </div>
              <div className="flex justify-between">
                <span className="text-slate-500">Timestamp:</span>
                <span className="text-slate-400">{new Date(selectedJob.timestamp).toISOString()}</span>
              </div>
            </div>

            <div>
              <span className="text-[11px] font-mono text-slate-400 block mb-1.5">PAYLOAD DATA (JSON):</span>
              <pre className="p-3 bg-[#07090e] rounded-xl border border-slate-800 text-[11px] font-mono text-emerald-400 overflow-x-auto">
                {JSON.stringify(selectedJob.data || { sample: 'task_payload' }, null, 2)}
              </pre>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
