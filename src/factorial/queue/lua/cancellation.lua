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

-- If parked or in backoff or pending tool results, always delete & remove (safe even if not present)
redis.call('DEL', pending_tool_results_key)
redis.call('ZREM', queue_pending_key, task_id)
redis.call('ZREM', queue_backoff_key, task_id)

if status == "pending_tool_results" or status == "pending_child_tasks" then
    if status == "pending_child_tasks" then
        -- The pending_child_task_results_key is stored as a **hash** where each
        -- field name is a child task ID and the value is either a PENDING sentinel
        -- or the serialized final output once the child task completes. We only
        -- care about the *IDs*, so read the hash field names with HKEYS.
        local child_task_ids = redis.call('HKEYS', pending_child_task_results_key)
        if #child_task_ids > 0 then
            redis.call('SADD', pending_cancellations_key, unpack(child_task_ids))
        end
        redis.call('DEL', pending_child_task_results_key)
    end

    -- Update task status to cancelled
    redis.call('HSET', task_statuses_key, task_id, 'cancelled')
    -- Add to cancelled queue
    redis.call('ZADD', queue_cancelled_key, timestamp, task_id)
    -- Update timeline metrics
    inc_metrics(
        { agent_metrics_bucket_key, global_metrics_bucket_key },
        { 'cancelled', pickups, retries, meta_json, metrics_ttl }
    )

    return { true, status, "Task cancelled", owner_id or "" }
elseif status == "backoff" then
    -- Task is in backoff queue - cancel immediately
    redis.call('HSET', task_statuses_key, task_id, 'cancelled')
    -- Add to cancelled queue
    redis.call('ZADD', queue_cancelled_key, timestamp, task_id)
    -- Update timeline metrics
    inc_metrics(
        { agent_metrics_bucket_key, global_metrics_bucket_key },
        { 'cancelled', pickups, retries, meta_json, metrics_ttl }
    )

    return { true, status, "Task cancelled", owner_id or "" }
else
    -- Task is in queued, active, or processing state
    -- Add to cancellation set for worker to handle
    -- No event data to return, as the task will be cancelled by the worker
    redis.call('SADD', pending_cancellations_key, task_id)
    return { true, status, "Task added to cancellation set", "" }
end
