import { DAGNode, DAGWorkflow, Job } from './types.js';
import { HyperQueue } from './queue.js';
import { v4 as uuidv4 } from 'uuid';

export class DAGWorkflowEngine {
  private queue: HyperQueue;

  constructor(queue: HyperQueue) {
    this.queue = queue;
  }

  /**
   * Validate that the workflow is a valid Directed Acyclic Graph (no cyclic dependencies)
   * Using Kahn's Algorithm (Topological Sort)
   */
  public static validateDAG(nodes: DAGNode[]): { isValid: boolean; sortedOrder?: string[]; error?: string } {
    const inDegree = new Map<string, number>();
    const adjList = new Map<string, string[]>();
    const nodeMap = new Map<string, DAGNode>();

    for (const node of nodes) {
      nodeMap.set(node.id, node);
      inDegree.set(node.id, 0);
      adjList.set(node.id, []);
    }

    // Build adjacency list and in-degrees
    for (const node of nodes) {
      for (const depId of node.dependencies) {
        if (!nodeMap.has(depId)) {
          return { isValid: false, error: `Missing dependency: node '${node.id}' depends on non-existent node '${depId}'` };
        }
        adjList.get(depId)!.push(node.id);
        inDegree.set(node.id, (inDegree.get(node.id) || 0) + 1);
      }
    }

    // Queue nodes with in-degree 0 (roots)
    const queue: string[] = [];
    inDegree.forEach((deg, nodeId) => {
      if (deg === 0) queue.push(nodeId);
    });

    const sortedOrder: string[] = [];
    while (queue.length > 0) {
      const u = queue.shift()!;
      sortedOrder.push(u);

      for (const v of adjList.get(u) || []) {
        inDegree.set(v, inDegree.get(v)! - 1);
        if (inDegree.get(v) === 0) {
          queue.push(v);
        }
      }
    }

    if (sortedOrder.length !== nodes.length) {
      return { isValid: false, error: 'Cycle detected in workflow graph! Workflows must be Directed Acyclic Graphs (DAG).' };
    }

    return { isValid: true, sortedOrder };
  }

  /**
   * Dispatch workflow to Redis and queue root nodes (nodes with 0 dependencies)
   */
  async submitWorkflow(name: string, nodes: DAGNode[]): Promise<{ workflowId: string; rootJobIds: string[] }> {
    const validation = DAGWorkflowEngine.validateDAG(nodes);
    if (!validation.isValid) {
      throw new Error(`DAG Validation Failed: ${validation.error}`);
    }

    const workflowId = uuidv4();
    const workflowKey = `hyperqueue:workflow:${workflowId}`;

    const pipeline = this.queue.redis.pipeline();
    pipeline.hset(workflowKey, 'name', name);
    pipeline.hset(workflowKey, 'status', 'running');
    pipeline.hset(workflowKey, 'createdAt', Date.now().toString());
    pipeline.hset(workflowKey, 'totalNodes', nodes.length.toString());
    pipeline.hset(workflowKey, 'completedNodes', '0');

    // Store node dependencies and dependents
    for (const node of nodes) {
      pipeline.hset(`${workflowKey}:node:${node.id}`, {
        data: JSON.stringify(node.data),
        name: node.name,
        dependencies: JSON.stringify(node.dependencies),
        remainingDeps: node.dependencies.length.toString(),
        status: 'pending',
      });
    }

    await pipeline.exec();

    // Enqueue all root nodes (inDegree == 0)
    const rootNodes = nodes.filter((n) => n.dependencies.length === 0);
    const rootJobIds: string[] = [];

    for (const root of rootNodes) {
      const job = await this.queue.add(root.name, root.data, {
        ...root.opts,
      });

      // Link job to workflow step
      await this.queue.redis.hset('hyperqueue:jobs', job.id, JSON.stringify({
        ...job,
        workflowId,
        stepId: root.id,
      }));

      rootJobIds.push(job.id);
    }

    return { workflowId, rootJobIds };
  }
}
