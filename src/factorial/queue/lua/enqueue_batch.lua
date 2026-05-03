---@diagnostic disable: undefined-global
--[[
-- Enqueue a batch of tasks and initialize batch bookkeeping atomically.
--
-- Batch creation needs consistent task writes, queue inserts, and batch
-- metadata setup while supporting deterministic replay/conflict behavior for
-- repeated requests.
--
-- State transitions:
-- - per-task: absent task id -> queued (and RPUSH to main queue)
-- - per-task: existing task id -> replay-safe (no duplicate enqueue)
-- - per-batch: first creation -> batch metadata/status initialized as active
-- - repeated same request -> replay
-- - same request key with different payload hash -> conflict
]]--
-- batch_enqueue.lua
-- Atomic script that enqueues **N** tasks for a single agent and initialises batch bookkeeping.
--
-- KEYS
--   [1]  agent_queue_key                 (LIST)
--   [2]  task_statuses_key               (HASH)
--   [3]  task_agents_key                 (HASH)
--   [4]  task_payloads_key               (HASH)
--   [5]  task_pickups_key                (HASH)
--   [6]  task_retries_key                (HASH)
--   [7]  task_metas_key                  (HASH)
--   [8]  batch_tasks_key                 (SET)   all task_ids for the batch
--   [9]  batch_meta_key                  (HASH)  batch_id -> meta_json
--   [10] batch_remaining_tasks_key       (HASH)  batch_id -> remaining task_ids json
--   [11] batch_progress_key              (HASH)  batch_id -> progress number
--   [12] batch_enqueue_idempotency_key   (STRING, optional)
--
-- ARGV
--   [1] batch_id
--   [2] owner_id
--   [3] created_at  (float timestamp)
--   [4] agent_name
--   [5] tasks_json  (JSON array of objects {id = <id>, payload_json = <payloadJson>})
--   [6] base_task_meta_json (JSON object with task metadata defaults)
--   [7] batch_meta_json (JSON object {owner_id = <ownerId>, created_at = <createdAt>, total_tasks = <totalTasks>, max_progress = <maxProgress>})
--   [8] request_hash (string, optional)
--   [9] ttl_seconds (int, optional)
--   [10] idempotency_enabled ("1" | "0", optional)
--
-- Returns: { decision, batch_id, task_ids_json, request_hash }
--   decision: "enqueued" | "replay" | "conflict"

local batch_id                  = ARGV[1]
local owner_id                  = ARGV[2]
local created_at                = tonumber(ARGV[3])
local agent_name                = ARGV[4]
local tasks_json                = cjson.decode(ARGV[5])
local base_task_meta_json       = ARGV[6]
local batch_meta_json           = ARGV[7]
local request_hash              = ARGV[8] or ""
local ttl_seconds               = tonumber(ARGV[9]) or 0
local idempotency_enabled       = ARGV[10] == "1"

local agent_queue_key           = KEYS[1]
local task_statuses_key         = KEYS[2]
local task_agents_key           = KEYS[3]
local task_payloads_key         = KEYS[4]
local task_pickups_key          = KEYS[5]
local task_retries_key          = KEYS[6]
local task_metas_key            = KEYS[7]
local batch_tasks_key           = KEYS[8]
local batch_meta_key            = KEYS[9]
local batch_remaining_tasks_key = KEYS[10]
local batch_progress_key        = KEYS[11]
local batch_enqueue_idempotency_key = KEYS[12]

if ttl_seconds < 1 then
    ttl_seconds = 1
end

if not batch_enqueue_idempotency_key or batch_enqueue_idempotency_key == "" then
    idempotency_enabled = false
end

local function persist_idem_record(stored_batch_id, stored_task_ids)
    local record = {
        batch_id = stored_batch_id,
        task_ids = stored_task_ids,
        request_hash = request_hash,
    }
    redis.call('SET', batch_enqueue_idempotency_key, cjson.encode(record), 'EX', ttl_seconds)
end

if idempotency_enabled then
    local existing_raw = redis.call('GET', batch_enqueue_idempotency_key)
    if existing_raw then
        local ok, existing = pcall(cjson.decode, existing_raw)
        if ok and type(existing) == 'table' then
            local existing_hash = existing.request_hash
            local existing_batch_id = existing.batch_id
            local existing_task_ids = existing.task_ids
            local existing_task_ids_valid = type(existing_task_ids) == 'table'

            if type(existing_hash) == 'string' and type(existing_batch_id) == 'string' and existing_task_ids_valid then
                if existing_hash ~= request_hash then
                    return { 'conflict', existing_batch_id, cjson.encode(existing_task_ids), existing_hash }
                end

                if redis.call('HEXISTS', batch_meta_key, existing_batch_id) == 1 then
                    redis.call('EXPIRE', batch_enqueue_idempotency_key, ttl_seconds)
                    return { 'replay', existing_batch_id, cjson.encode(existing_task_ids), existing_hash }
                end

                -- Stale idempotency record: reuse prior deterministic IDs.
                batch_id = existing_batch_id
                if #existing_task_ids == #tasks_json then
                    for i = 1, #tasks_json do
                        tasks_json[i]["id"] = existing_task_ids[i]
                    end
                end
            end
        end
    end
end

local batch_preexisting = redis.call('HEXISTS', batch_meta_key, batch_id) == 1
local task_ids = {}

for i = 1, #tasks_json do
    local task         = tasks_json[i]
    local task_id      = task["id"]
    local payload_json = task["payload_json"]

    -- Idempotency: if this task ID already exists, do not enqueue/mutate it.
    if redis.call('HEXISTS', task_statuses_key, task_id) == 0 then
        -- Queue the task at the tail of the agent queue
        redis.call('RPUSH', agent_queue_key, task_id)

        -- Core task hashes (mirrors enqueue.lua)
        redis.call('HSET', task_statuses_key, task_id, 'queued')
        redis.call('HSET', task_agents_key, task_id, agent_name)
        redis.call('HSET', task_payloads_key, task_id, payload_json)
        redis.call('HSET', task_pickups_key, task_id, 0)
        redis.call('HSET', task_retries_key, task_id, 0)
        redis.call('HSET', task_metas_key, task_id, base_task_meta_json)
    end

    table.insert(task_ids, task_id)
end

-- Preserve existing batch progress/remaining bookkeeping on retries.
if not batch_preexisting then
    redis.call('HSET', batch_meta_key, batch_id, batch_meta_json)
    redis.call('HSET', batch_tasks_key, batch_id, cjson.encode(task_ids))
    redis.call('HSET', batch_remaining_tasks_key, batch_id, cjson.encode(task_ids))
    redis.call('HSET', batch_progress_key, batch_id, 0)
end

if idempotency_enabled then
    persist_idem_record(batch_id, task_ids)
end

if batch_preexisting then
    return { 'replay', batch_id, cjson.encode(task_ids), request_hash }
end

return { 'enqueued', batch_id, cjson.encode(task_ids), request_hash }
