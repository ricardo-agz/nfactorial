---@diagnostic disable: undefined-global
--[[
-- Materialize a resumed task and enqueue it exactly once.
--
-- Resuming a task atomically decides between new enqueue, replay of an already
-- materialized resumed task, or conflict on key/payload mismatch.
--
-- State transitions:
-- - resumed task missing -> queued (and RPUSH to main queue)
-- - same resume request seen before -> replay existing resumed task
-- - same request key reused with different payload hash -> conflict
]]--
local agent_queue_key = KEYS[1]
local task_statuses_key = KEYS[2]
local task_agents_key = KEYS[3]
local task_payloads_key = KEYS[4]
local task_pickups_key = KEYS[5]
local task_retries_key = KEYS[6]
local task_metas_key = KEYS[7]
local resume_idempotency_key = KEYS[8]

local task_id = ARGV[1]
local task_agent = ARGV[2]
local task_payload_json = ARGV[3]
local task_pickups = tonumber(ARGV[4])
local task_retries = tonumber(ARGV[5])
local task_meta_json = ARGV[6]
local request_hash = ARGV[7]
local source_task_id = ARGV[8]
local ttl_seconds = tonumber(ARGV[9]) or 0
local idempotency_enabled = ARGV[10] == "1"

if ttl_seconds < 1 then
    ttl_seconds = 1
end

local function enqueue_task_if_missing()
    -- If task already exists, treat as duplicate materialization attempt.
    if redis.call('HEXISTS', task_statuses_key, task_id) == 1 then
        return false
    end

    redis.call('RPUSH', agent_queue_key, task_id)
    redis.call('HSET', task_statuses_key, task_id, "queued")
    redis.call('HSET', task_agents_key, task_id, task_agent)
    redis.call('HSET', task_payloads_key, task_id, task_payload_json)
    redis.call('HSET', task_pickups_key, task_id, task_pickups)
    redis.call('HSET', task_retries_key, task_id, task_retries)
    redis.call('HSET', task_metas_key, task_id, task_meta_json)
    return true
end

if not idempotency_enabled then
    enqueue_task_if_missing()
    return { 'enqueued', task_id, request_hash }
end

local function persist_idem_record()
    local record = {
        source_task_id = source_task_id,
        resumed_task_id = task_id,
        request_hash = request_hash,
    }
    redis.call('SET', resume_idempotency_key, cjson.encode(record), 'EX', ttl_seconds)
end

local existing_raw = redis.call('GET', resume_idempotency_key)
if existing_raw then
    local ok, existing = pcall(cjson.decode, existing_raw)
    if ok and type(existing) == 'table' then
        local existing_hash = existing.request_hash
        local existing_resumed_task_id = existing.resumed_task_id
        if type(existing_hash) == 'string' and type(existing_resumed_task_id) == 'string' then
            if existing_hash ~= request_hash then
                return { 'conflict', existing_resumed_task_id, existing_hash }
            end
            if redis.call('HEXISTS', task_statuses_key, existing_resumed_task_id) == 1 then
                redis.call('EXPIRE', resume_idempotency_key, ttl_seconds)
                return { 'replay', existing_resumed_task_id, existing_hash }
            end
        end
    end
end

local enqueued = enqueue_task_if_missing()
if enqueued then
    persist_idem_record()
    return { 'enqueued', task_id, request_hash }
end

-- Deterministic task already exists (e.g., stale or expired idempotency record).
persist_idem_record()
return { 'replay', task_id, request_hash }
