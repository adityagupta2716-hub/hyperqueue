import test from 'node:test';
import assert from 'node:assert/strict';
import { DAGWorkflowEngine } from './dag.js';
import { DAGNode } from './types.js';

test('DAG Validation - Valid Linear Pipeline', () => {
  const nodes: DAGNode[] = [
    { id: 'step-1', name: 'extract', data: {}, dependencies: [] },
    { id: 'step-2', name: 'transform', data: {}, dependencies: ['step-1'] },
    { id: 'step-3', name: 'load', data: {}, dependencies: ['step-2'] },
  ];

  const result = DAGWorkflowEngine.validateDAG(nodes);
  assert.equal(result.isValid, true);
  assert.deepEqual(result.sortedOrder, ['step-1', 'step-2', 'step-3']);
});

test('DAG Validation - Valid Diamond Parallel Graph', () => {
  const nodes: DAGNode[] = [
    { id: 'A', name: 'source', data: {}, dependencies: [] },
    { id: 'B1', name: 'worker-1', data: {}, dependencies: ['A'] },
    { id: 'B2', name: 'worker-2', data: {}, dependencies: ['A'] },
    { id: 'C', name: 'aggregator', data: {}, dependencies: ['B1', 'B2'] },
  ];

  const result = DAGWorkflowEngine.validateDAG(nodes);
  assert.equal(result.isValid, true);
  assert.equal(result.sortedOrder?.[0], 'A');
  assert.equal(result.sortedOrder?.[3], 'C');
});

test('DAG Validation - Detects Cycles (Invalid DAG)', () => {
  const cyclicNodes: DAGNode[] = [
    { id: '1', name: 'first', data: {}, dependencies: ['3'] },
    { id: '2', name: 'second', data: {}, dependencies: ['1'] },
    { id: '3', name: 'third', data: {}, dependencies: ['2'] }, // Cycle: 1 -> 2 -> 3 -> 1
  ];

  const result = DAGWorkflowEngine.validateDAG(cyclicNodes);
  assert.equal(result.isValid, false);
  assert.match(result.error || '', /Cycle detected/);
});

test('DAG Validation - Missing Dependency Error', () => {
  const brokenNodes: DAGNode[] = [
    { id: 'step-1', name: 'start', data: {}, dependencies: ['non-existent-id'] },
  ];

  const result = DAGWorkflowEngine.validateDAG(brokenNodes);
  assert.equal(result.isValid, false);
  assert.match(result.error || '', /Missing dependency/);
});
