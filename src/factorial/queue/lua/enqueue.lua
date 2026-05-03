---@diagnostic disable: undefined-global
--[[
-- Enqueue exactly one task with deterministic replay semantics.
--
-- Single-task enqueue atomically decides between fresh enqueue, replay of an
-- existing request, or conflict when the same request key is reused with
-- different payload.
--
-- State transitions:
-- - absent task id -> queued (and RPUSH to main queue)
-- - existing deterministic task id -> replay (no duplicate enqueue)
-- - request key reused with different payload hash -> conflict
]]--
local agent_queue_key = KEYS[1]
local task_statuses_key = KEYS[2]
local task_agents_key = KEYS[3]
local task_payloads_key = KEYS[4]
local task_pickups_key = KEYS[5]
local task_retries_key = KEYS[6]
local task_metas_key = KEYS[7]
local enqueue_idempotency_key = KEYS[8]

local task_id = ARGV[1]
local task_agent = ARGV[2]
local task_payload_json = ARGV[3]
local task_pickups = tonumber(ARGV[4])
local task_retries = tonumber(ARGV[5])
local task_meta_json = ARGV[6]
local request_hash = ARGV[7] or ""
local ttl_seconds = tonumber(ARGV[8]) or 0
local idempotency_enabled = ARGV[9] == "1"

if ttl_seconds < 1 then
    ttl_seconds = 1
end

if not enqueue_idempotency_key or enqueue_idempotency_key == "" then
    idempotency_enabled = false
end

local function enqueue_task_if_missing(enqueue_task_id)
    -- Idempotency: if a task with the same ID already exists, do not enqueue again.
    if redis.call('HEXISTS', task_statuses_key, enqueue_task_id) == 1 then
        return false
    end

    -- Push task_id to the back of the agent queue
    redis.call('RPUSH', agent_queue_key, enqueue_task_id)

    -- Set task data
    redis.call('HSET', task_statuses_key, enqueue_task_id, "queued")
    redis.call('HSET', task_agents_key, enqueue_task_id, task_agent)
    redis.call('HSET', task_payloads_key, enqueue_task_id, task_payload_json)
    redis.call('HSET', task_pickups_key, enqueue_task_id, task_pickups)
    redis.call('HSET', task_retries_key, enqueue_task_id, task_retries)
    redis.call('HSET', task_metas_key, enqueue_task_id, task_meta_json)
    return true
end

if not idempotency_enabled then
    local enqueued = enqueue_task_if_missing(task_id)
    if enqueued then
        return { 'enqueued', task_id, request_hash }
    end
    return { 'replay', task_id, request_hash }
end

local function persist_idem_record(stored_task_id)
    local record = {
        task_id = stored_task_id,
        request_hash = request_hash,
    }
    redis.call('SET', enqueue_idempotency_key, cjson.encode(record), 'EX', ttl_seconds)
end

local resolved_task_id = task_id
local existing_raw = redis.call('GET', enqueue_idempotency_key)
if existing_raw then
    local ok, existing = pcall(cjson.decode, existing_raw)
    if ok and type(existing) == 'table' then
        local existing_hash = existing.request_hash
        local existing_task_id = existing.task_id
        if type(existing_hash) == 'string' and type(existing_task_id) == 'string' then
            if existing_hash ~= request_hash then
                return { 'conflict', existing_task_id, existing_hash }
            end

            resolved_task_id = existing_task_id
            if redis.call('HEXISTS', task_statuses_key, resolved_task_id) == 1 then
                redis.call('EXPIRE', enqueue_idempotency_key, ttl_seconds)
                return { 'replay', resolved_task_id, existing_hash }
            end
        end
    end
end

local enqueued = enqueue_task_if_missing(resolved_task_id)
persist_idem_record(resolved_task_id)

if enqueued then
    return { 'enqueued', resolved_task_id, request_hash }
end

return { 'replay', resolved_task_id, request_hash }
