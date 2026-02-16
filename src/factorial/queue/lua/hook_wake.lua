local queue_main_key           = KEYS[1]
local queue_pending_key        = KEYS[2]
local queue_orphaned_key       = KEYS[3]
local task_statuses_key        = KEYS[4]
local task_agents_key          = KEYS[5]
local task_payloads_key        = KEYS[6]
local task_pickups_key         = KEYS[7]
local task_retries_key         = KEYS[8]
local task_metas_key           = KEYS[9]
local hook_runtime_ready_key   = KEYS[10]

local task_id                  = ARGV[1]
local tool_call_id             = ARGV[2]
local session_id               = ARGV[3]

local time_result              = redis.call('TIME')
local timestamp                = tonumber(time_result[1]) + (tonumber(time_result[2]) / 1000000)

local task_result = load_task(
    { task_statuses_key, task_agents_key, task_payloads_key, task_pickups_key, task_retries_key, task_metas_key },
    { task_id }
)

if task_result.state == "missing" then
    redis.call('ZADD', queue_orphaned_key, timestamp, task_id)
    return { false, "missing" }
elseif task_result.state == "corrupted" then
    return { false, "corrupted" }
end

local status = task_result.status
if status == "completed" or status == "failed" or status == "cancelled" then
    return { false, "inactive" }
end

-- Persist the wake intent first so worker can consume it even if the task
-- is already active/processing due to an earlier wake.
redis.call('HSET', hook_runtime_ready_key, tool_call_id, session_id)

if status == "pending_tool_results" then
    redis.call('ZREM', queue_pending_key, task_id)
    redis.call('HSET', task_statuses_key, task_id, "active")
    redis.call('LPUSH', queue_main_key, task_id)
    return { true, "ok" }
end

-- If not pending, intent is still stored and will be consumed next worker pass.
return { false, "already_active" }
