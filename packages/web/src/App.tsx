import React, { useState, useEffect } from 'react';
import { Activity, Server, Zap, CheckCircle2, Clock, ShieldAlert, Play, RefreshCw } from 'lucide-react';

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
}

export default function App() {
  const [stats, setStats] = useState<QueueStats>({ ready: 0, delayed: 0, active: 0, completed: 0, dlq: 0 });
  const [events, setEvents] = useState<EventItem[]>([]);
  const [taskName, setTaskName] = useState('process-image-pipeline');
  const [priority, setPriority] = useState('high');
  const [delayMs, setDelayMs] = useState(0);
  const [isSubmitting, setIsSubmitting] = useState(false);
  const [connected, setConnected] = useState(false);

  useEffect(() => {
    // Connect to WebSocket Server
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
          };
          setEvents((prev) => [newEvent, ...prev.slice(0, 19)]);
          fetchStats();
        }
      } catch (e) {
        console.error('WS parse error:', e);
      }
    };

    fetchStats();
    const interval = setInterval(fetchStats, 3000);

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
    } catch {
      // Backend might be offline during dev
    }
  };

  const handleDispatch = async (e: React.FormEvent) => {
    e.preventDefault();
    setIsSubmitting(true);
    try {
      await fetch('http://localhost:4000/api/jobs', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          name: taskName,
          data: { payload: 'simulation', timestamp: Date.now() },
          priority,
          delayMs: Number(delayMs),
        }),
      });
      fetchStats();
    } catch (err) {
      console.error('Dispatch error:', err);
    } finally {
      setIsSubmitting(false);
    }
  };

  return (
    <div className="min-h-screen bg-slate-950 text-slate-100 flex flex-col">
      {/* Top Navbar */}
      <header className="border-b border-slate-800 bg-slate-900/50 backdrop-blur px-6 py-4 flex items-center justify-between">
        <div className="flex items-center gap-3">
          <div className="p-2 bg-indigo-600 rounded-lg shadow-lg shadow-indigo-500/30">
            <Zap className="w-5 h-5 text-white" />
          </div>
          <div>
            <h1 className="text-xl font-bold tracking-tight text-white flex items-center gap-2">
              HyperQueue
              <span className="text-xs bg-indigo-500/20 text-indigo-300 font-mono px-2 py-0.5 rounded border border-indigo-500/30">
                v1.0.0
              </span>
            </h1>
            <p className="text-xs text-slate-400">Distributed DAG Workflow & Task Orchestration Engine</p>
          </div>
        </div>

        <div className="flex items-center gap-4">
          <div className="flex items-center gap-2 text-xs font-mono">
            <span className={`w-2.5 h-2.5 rounded-full ${connected ? 'bg-emerald-500 shadow-lg shadow-emerald-500/50 animate-pulse' : 'bg-rose-500'}`} />
            {connected ? 'Live WS Connected' : 'Connecting to Node Cluster...'}
          </div>
          <button
            onClick={fetchStats}
            className="p-2 bg-slate-800 hover:bg-slate-700 rounded-lg text-slate-300 transition"
          >
            <RefreshCw className="w-4 h-4" />
          </button>
        </div>
      </header>

      {/* Main Content Area */}
      <main className="flex-1 p-6 max-w-7xl w-full mx-auto space-y-6">
        {/* Metric Cards Grid */}
        <div className="grid grid-cols-1 md:grid-cols-5 gap-4">
          <div className="bg-slate-900 border border-slate-800 p-4 rounded-xl shadow-sm">
            <div className="flex items-center justify-between text-slate-400 mb-2">
              <span className="text-xs font-medium uppercase tracking-wider">Ready Queue</span>
              <Activity className="w-4 h-4 text-sky-400" />
            </div>
            <div className="text-2xl font-bold text-sky-400 font-mono">{stats.ready}</div>
            <div className="text-xs text-slate-500 mt-1">Awaiting worker lease</div>
          </div>

          <div className="bg-slate-900 border border-slate-800 p-4 rounded-xl shadow-sm">
            <div className="flex items-center justify-between text-slate-400 mb-2">
              <span className="text-xs font-medium uppercase tracking-wider">Active In-Flight</span>
              <Server className="w-4 h-4 text-indigo-400" />
            </div>
            <div className="text-2xl font-bold text-indigo-400 font-mono">{stats.active}</div>
            <div className="text-xs text-slate-500 mt-1">Executing on worker pool</div>
          </div>

          <div className="bg-slate-900 border border-slate-800 p-4 rounded-xl shadow-sm">
            <div className="flex items-center justify-between text-slate-400 mb-2">
              <span className="text-xs font-medium uppercase tracking-wider">Delayed / Scheduled</span>
              <Clock className="w-4 h-4 text-amber-400" />
            </div>
            <div className="text-2xl font-bold text-amber-400 font-mono">{stats.delayed}</div>
            <div className="text-xs text-slate-500 mt-1">Timer active in ZSET</div>
          </div>

          <div className="bg-slate-900 border border-slate-800 p-4 rounded-xl shadow-sm">
            <div className="flex items-center justify-between text-slate-400 mb-2">
              <span className="text-xs font-medium uppercase tracking-wider">Completed</span>
              <CheckCircle2 className="w-4 h-4 text-emerald-400" />
            </div>
            <div className="text-2xl font-bold text-emerald-400 font-mono">{stats.completed}</div>
            <div className="text-xs text-slate-500 mt-1">Successfully resolved</div>
          </div>

          <div className="bg-slate-900 border border-slate-800 p-4 rounded-xl shadow-sm">
            <div className="flex items-center justify-between text-slate-400 mb-2">
              <span className="text-xs font-medium uppercase tracking-wider">Dead Letter (DLQ)</span>
              <ShieldAlert className="w-4 h-4 text-rose-400" />
            </div>
            <div className="text-2xl font-bold text-rose-400 font-mono">{stats.dlq}</div>
            <div className="text-xs text-slate-500 mt-1">Exceeded max retries</div>
          </div>
        </div>

        {/* 2-Column Split: Dispatcher & Live Log */}
        <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
          {/* Dispatcher Form */}
          <div className="bg-slate-900 border border-slate-800 rounded-xl p-5 shadow-sm space-y-4">
            <div className="flex items-center gap-2 border-b border-slate-800 pb-3">
              <Play className="w-4 h-4 text-indigo-400" />
              <h2 className="font-semibold text-sm">Dispatch Benchmark Task</h2>
            </div>

            <form onSubmit={handleDispatch} className="space-y-4 text-sm">
              <div>
                <label className="block text-xs font-medium text-slate-400 mb-1">Task Identifier</label>
                <input
                  type="text"
                  value={taskName}
                  onChange={(e) => setTaskName(e.target.value)}
                  className="w-full bg-slate-950 border border-slate-800 rounded-lg px-3 py-2 text-slate-200 focus:outline-none focus:border-indigo-500 font-mono text-xs"
                />
              </div>

              <div>
                <label className="block text-xs font-medium text-slate-400 mb-1">Priority Level</label>
                <select
                  value={priority}
                  onChange={(e) => setPriority(e.target.value)}
                  className="w-full bg-slate-950 border border-slate-800 rounded-lg px-3 py-2 text-slate-200 focus:outline-none focus:border-indigo-500 text-xs"
                >
                  <option value="critical">Critical (Weight: 1)</option>
                  <option value="high">High (Weight: 10)</option>
                  <option value="normal">Normal (Weight: 100)</option>
                  <option value="low">Low (Weight: 1000)</option>
                </select>
              </div>

              <div>
                <label className="block text-xs font-medium text-slate-400 mb-1">Delay (ms)</label>
                <input
                  type="number"
                  value={delayMs}
                  onChange={(e) => setDelayMs(Number(e.target.value))}
                  className="w-full bg-slate-950 border border-slate-800 rounded-lg px-3 py-2 text-slate-200 focus:outline-none focus:border-indigo-500 font-mono text-xs"
                />
              </div>

              <button
                type="submit"
                disabled={isSubmitting}
                className="w-full py-2 px-4 bg-indigo-600 hover:bg-indigo-500 disabled:opacity-50 text-white font-medium rounded-lg shadow-lg shadow-indigo-600/30 transition text-xs flex items-center justify-center gap-2"
              >
                <Zap className="w-3.5 h-3.5" />
                {isSubmitting ? 'Enqueueing...' : 'Push to Distributed Queue'}
              </button>
            </form>
          </div>

          {/* Live Event Stream */}
          <div className="lg:col-span-2 bg-slate-900 border border-slate-800 rounded-xl p-5 shadow-sm space-y-4">
            <div className="flex items-center justify-between border-b border-slate-800 pb-3">
              <div className="flex items-center gap-2">
                <Activity className="w-4 h-4 text-emerald-400" />
                <h2 className="font-semibold text-sm">Real-Time Redis Stream Telemetry</h2>
              </div>
              <span className="text-xs text-slate-500 font-mono">Live Pub/Sub</span>
            </div>

            <div className="h-72 overflow-y-auto space-y-2 pr-2 font-mono text-xs">
              {events.length === 0 ? (
                <div className="h-full flex items-center justify-center text-slate-500">
                  Waiting for task dispatch events...
                </div>
              ) : (
                events.map((evt) => (
                  <div
                    key={evt.id}
                    className="p-2.5 bg-slate-950 border border-slate-800/80 rounded-lg flex items-center justify-between hover:border-slate-700 transition"
                  >
                    <div className="flex items-center gap-3">
                      <span className="px-1.5 py-0.5 rounded text-[10px] font-bold bg-indigo-500/20 text-indigo-300 border border-indigo-500/30">
                        {evt.type}
                      </span>
                      <span className="text-slate-200 font-semibold">{evt.name}</span>
                      <span className="text-slate-500 text-[11px] truncate max-w-[120px]">{evt.id}</span>
                    </div>

                    <div className="flex items-center gap-2">
                      <span className={`px-2 py-0.5 rounded text-[10px] font-medium ${
                        evt.priority === 'critical' ? 'bg-rose-500/20 text-rose-300' :
                        evt.priority === 'high' ? 'bg-amber-500/20 text-amber-300' : 'bg-sky-500/20 text-sky-300'
                      }`}>
                        {evt.priority}
                      </span>
                      <span className="text-slate-500 text-[11px]">
                        {new Date(evt.timestamp).toLocaleTimeString()}
                      </span>
                    </div>
                  </div>
                ))
              )}
            </div>
          </div>
        </div>
      </main>
    </div>
  );
}
