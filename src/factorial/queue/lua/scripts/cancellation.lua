--[[
-- Cancel a single task across all queue states.
--
-- Cancellation may arrive while a task is queued, running, parked, or waiting
-- on tools/children. This script performs required cleanup and transition in
-- one atomic operation.
--
-- State transitions:
-- - pending_tool_results | pending_child_tasks | backoff | paused -> cancelled
-- - queued | active | processing -> no immediate status change; task is added
--   to pending_cancellations for worker-side cancellation
-- - missing task data -> orphaned queue marker
-- - completed | failed | cancelled -> no transition (already terminal)
]]--
local queue_cancelled_key = KEYS[1]
local queue_backoff_key = KEYS[2]
local queue_orphaned_key = KEYS[3]
local queue_pending_key = KEYS[4]
local pending_cancellations_key = KEYS[5]
local task_statuses_key = KEYS[6]
local task_agents_key = KEYS[7]
local task_payloads_key = KEYS[8]
local task_pickups_key = KEYS[9]
local task_retries_key = KEYS[10]
local task_metas_key = KEYS[11]
local pending_tool_results_key = KEYS[12]
local pending_child_task_results_key = KEYS[13]
local agent_metrics_bucket_key = KEYS[14]
local global_metrics_bucket_key = KEYS[15]
local queue_scheduled_key = KEYS[16]
local scheduled_wait_meta_key = KEYS[17]
local pending_child_wait_ids_key = KEYS[18]
local activity_wait_meta_key = KEYS[19]
local queue_main_key_template = KEYS[20]
local queue_pending_key_template = KEYS[21]
local queue_scheduled_key_template = KEYS[22]
local task_steering_key_template = KEYS[23]
local message_seq_key = KEYS[24]

local task_id = ARGV[1]
local metrics_ttl = tonumber(ARGV[2])

local time_result = redis.call('TIME')
local timestamp = tonumber(time_result[1]) + (tonumber(time_result[2]) / 1000000)


local task_result = load_task(
    { task_statuses_key, task_agents_key, task_payloads_key, task_pickups_key, task_retries_key, task_metas_key },
    { task_id }
)

local status = task_result.status

if task_result.state == "missing" then
    -- Task is missing, add to orphaned queue
    redis.call('ZADD', queue_orphaned_key, timestamp, task_id)
    return { false, "", "Task not found", "" }
elseif task_result.state == "corrupted" then
    return { false, task_result.status or "", "Task data is corrupted", "" }
elseif status == "completed" or status == "failed" or status == "cancelled" then
    return { false, status, "Task already in terminal state", "" }
end

local meta_json = task_result.meta
local agent = task_result.agent
local pickups = task_result.pickups
local retries = task_result.retries
local meta = cjson.decode(meta_json)
local owner_id = meta.owner_id
local parent_task_id = meta.parent_id

local function wake_parent_if_waiting_activity()
    if not parent_task_id or parent_task_id == cjson.null then
        return false
    end
    if not activity_wait_meta_key or activity_wait_meta_key == "" then
        return false
    end
    if not queue_main_key_template or queue_main_key_template == "" then
        return false
    end
    if not queue_pending_key_template or queue_pending_key_template == "" then
        return false
    end
    if not task_steering_key_template or task_steering_key_template == "" then
        return false
    end
    if not message_seq_key or message_seq_key == "" then
        return false
    end
    return wake_parent_on_child_terminal(
        {
            task_statuses_key,
            task_agents_key,
            queue_main_key_template,
            queue_pending_key_template,
            activity_wait_meta_key,
            task_steering_key_template,
            message_seq_key,
            queue_scheduled_key_template,
            scheduled_wait_meta_key,
        },
        {
            parent_task_id,
            task_id,
        }
    )
end

-- If parked or in backoff or pending tool results, always delete & remove (safe even if not present)
redis.call('DEL', pending_tool_results_key)
redis.call('ZREM', queue_pending_key, task_id)
redis.call('ZREM', queue_backoff_key, task_id)
if queue_scheduled_key and queue_scheduled_key ~= "" then
    redis.call('ZREM', queue_scheduled_key, task_id)
end
if scheduled_wait_meta_key and scheduled_wait_meta_key ~= "" then
    redis.call('HDEL', scheduled_wait_meta_key, task_id)
end
if activity_wait_meta_key and activity_wait_meta_key ~= "" then
    redis.call('HDEL', activity_wait_meta_key, task_id)
end

if status == "pending_tool_results" or status == "pending_child_tasks" then
    if status == "pending_child_tasks" then
        -- Prefer the explicit join set for the currently awaited children.
        local child_task_ids = {}
        if pending_child_wait_ids_key and pending_child_wait_ids_key ~= "" then
            child_task_ids = redis.call('SMEMBERS', pending_child_wait_ids_key)
        else
            child_task_ids = redis.call('HKEYS', pending_child_task_results_key)
        end
        if #child_task_ids > 0 then
            redis.call('SADD', pending_cancellations_key, unpack(child_task_ids))
        end
        if pending_child_wait_ids_key and pending_child_wait_ids_key ~= "" then
            if #child_task_ids > 0 then
                redis.call('HDEL', pending_child_task_results_key, unpack(child_task_ids))
            end
            redis.call('DEL', pending_child_wait_ids_key)
        else
            redis.call('DEL', pending_child_task_results_key)
        end
    end

    -- Update task status to cancelled
    redis.call('HSET', task_statuses_key, task_id, 'cancelled')
    -- Add to cancelled queue
    redis.call('ZADD', queue_cancelled_key, timestamp, task_id)
    -- Update timeline metrics
    inc_metrics(
        { agent_metrics_bucket_key, global_metrics_bucket_key },
        { 'cancelled', meta_json, metrics_ttl }
    )
    wake_parent_if_waiting_activity()

    return { true, status, "Task cancelled", owner_id or "" }
elseif status == "backoff" or status == "paused" then
    -- Task is in a parked queue (backoff/scheduled) - cancel immediately
    redis.call('HSET', task_statuses_key, task_id, 'cancelled')
    -- Add to cancelled queue
    redis.call('ZADD', queue_cancelled_key, timestamp, task_id)
    -- Update timeline metrics
    inc_metrics(
        { agent_metrics_bucket_key, global_metrics_bucket_key },
        { 'cancelled', meta_json, metrics_ttl }
    )
    wake_parent_if_waiting_activity()

    return { true, status, "Task cancelled", owner_id or "" }
else
    -- Task is in queued, active, or processing state
    -- Add to cancellation set for worker to handle
    -- No event data to return, as the task will be cancelled by the worker
    redis.call('SADD', pending_cancellations_key, task_id)
    return { true, status, "Task added to cancellation set", "" }
end
