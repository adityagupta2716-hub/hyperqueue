/**
 * Atomic Redis Lua Scripts for HyperQueue
 * 
 * In high-concurrency environments, multi-step Redis commands (GET then SET)
 * suffer from race conditions. Lua scripts execute atomically in Redis single-threaded
 * event loop, guaranteeing strict linearizability and zero double-processing.
 */

export const ACQUIRE_LOCK_LUA = `
-- KEYS[1]: lock key
-- ARGV[1]: lock owner token
-- ARGV[2]: TTL in milliseconds
if redis.call("SET", KEYS[1], ARGV[1], "NX", "PX", ARGV[2]) then
  return 1
else
  return 0
end
`;

export const RELEASE_LOCK_LUA = `
-- KEYS[1]: lock key
-- ARGV[1]: lock owner token
-- Safely release lock ONLY if the token matches (prevents releasing an expired lock acquired by another worker)
if redis.call("GET", KEYS[1]) == ARGV[1] then
  return redis.call("DEL", KEYS[1])
else
  return 0
end
`;

export const EXTEND_LOCK_LUA = `
-- KEYS[1]: lock key
-- ARGV[1]: lock owner token
-- ARGV[2]: additional TTL in milliseconds
if redis.call("GET", KEYS[1]) == ARGV[1] then
  return redis.call("PEXPIRE", KEYS[1], ARGV[2])
else
  return 0
end
`;

export const POP_READY_JOB_LUA = `
-- KEYS[1]: ready sorted set (key: hyperqueue:{queue}:ready, score: priority_weight)
-- KEYS[2]: delayed sorted set (key: hyperqueue:{queue}:delayed, score: timestamp)
-- KEYS[3]: active hash/set (key: hyperqueue:{queue}:active)
-- ARGV[1]: current timestamp
-- ARGV[2]: worker ID
-- ARGV[3]: visibility timeout in ms

-- Step 1: Promote ready delayed jobs to the ready sorted set
local delayedJobs = redis.call("ZRANGEBYSCORE", KEYS[2], 0, ARGV[1], "LIMIT", 0, 50)
for _, jobId in ipairs(delayedJobs) do
  local jobRaw = redis.call("HGET", "hyperqueue:jobs", jobId)
  if jobRaw then
    local priority = redis.call("HGET", "hyperqueue:job_priority", jobId) or "100"
    redis.call("ZADD", KEYS[1], priority, jobId)
    redis.call("ZREM", KEYS[2], jobId)
  else
    redis.call("ZREM", KEYS[2], jobId)
  end
end

-- Step 2: Pop highest priority job (lowest score in ZSET)
local jobs = redis.call("ZRANGE", KEYS[1], 0, 0)
if #jobs == 0 then
  return nil
end

local jobId = jobs[1]
redis.call("ZREM", KEYS[1], jobId)

-- Step 3: Mark as active and set visibility expiration in active ZSET
local visibilityExpiry = tonumber(ARGV[1]) + tonumber(ARGV[3])
redis.call("ZADD", KEYS[3], visibilityExpiry, jobId)
redis.call("HSET", "hyperqueue:job_workers", jobId, ARGV[2])

return jobId
`;

export const COMPLETE_JOB_LUA = `
-- KEYS[1]: active ZSET (key: hyperqueue:{queue}:active)
-- KEYS[2]: completed list/set (key: hyperqueue:{queue}:completed)
-- ARGV[1]: jobId
-- ARGV[2]: workerId
-- ARGV[3]: result JSON

redis.call("ZREM", KEYS[1], ARGV[1])
redis.call("HDEL", "hyperqueue:job_workers", ARGV[1])
redis.call("HSET", "hyperqueue:job_results", ARGV[1], ARGV[3])
redis.call("SADD", KEYS[2], ARGV[1])
return 1
`;

export const FAIL_JOB_LUA = `
-- KEYS[1]: active ZSET (key: hyperqueue:{queue}:active)
-- KEYS[2]: delayed ZSET (key: hyperqueue:{queue}:delayed)
-- KEYS[3]: dead-letter ZSET (key: hyperqueue:{queue}:dlq)
-- ARGV[1]: jobId
-- ARGV[2]: error string
-- ARGV[3]: isDeadLetter ("1" or "0")
-- ARGV[4]: retryTimestamp
-- ARGV[5]: priority

redis.call("ZREM", KEYS[1], ARGV[1])
redis.call("HDEL", "hyperqueue:job_workers", ARGV[1])
redis.call("HSET", "hyperqueue:job_errors", ARGV[1], ARGV[2])

if ARGV[3] == "1" then
  redis.call("ZADD", KEYS[3], ARGV[4], ARGV[1])
else
  redis.call("ZADD", KEYS[2], ARGV[4], ARGV[1])
end

return 1
`;
