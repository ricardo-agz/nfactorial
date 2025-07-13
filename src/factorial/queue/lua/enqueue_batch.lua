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
--
-- ARGV
--   [1] batch_id
--   [2] owner_id
--   [3] created_at  (float timestamp)
--   [4] agent_name
--   [5] tasks_json  (JSON array of objects {id = <id>, payload_json = <payloadJson>})
--   [6] base_task_meta_json (JSON object {max_turns = <maxTurns>})
--   [7] batch_meta_json (JSON object {owner_id = <ownerId>, created_at = <createdAt>, total_tasks = <totalTasks>, max_progress = <maxProgress>})
--
-- Returns: JSON array of task_ids that were enqueued.

local batch_id                  = ARGV[1]
local owner_id                  = ARGV[2]
local created_at                = tonumber(ARGV[3])
local agent_name                = ARGV[4]
local tasks_json                = cjson.decode(ARGV[5])
local base_task_meta_json       = ARGV[6]
local batch_meta_json           = ARGV[7]

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

local task_ids                  = {}

for i = 1, #tasks_json do
    local task         = tasks_json[i]
    local task_id      = task["id"]
    local payload_json = task["payload_json"]

    -- Queue the task at the tail of the agent queue
    redis.call('RPUSH', agent_queue_key, task_id)

    -- Core task hashes (mirrors enqueue.lua)
    redis.call('HSET', task_statuses_key, task_id, 'queued')
    redis.call('HSET', task_agents_key, task_id, agent_name)
    redis.call('HSET', task_payloads_key, task_id, payload_json)
    redis.call('HSET', task_pickups_key, task_id, 0)
    redis.call('HSET', task_retries_key, task_id, 0)
    redis.call('HSET', task_metas_key, task_id, base_task_meta_json)

    table.insert(task_ids, task_id)
end

redis.call('HSET', batch_meta_key, batch_id, batch_meta_json)
redis.call('HSET', batch_tasks_key, batch_id, cjson.encode(task_ids))
redis.call('HSET', batch_remaining_tasks_key, batch_id, cjson.encode(task_ids))
redis.call('HSET', batch_progress_key, batch_id, 0)

return cjson.encode(task_ids)
