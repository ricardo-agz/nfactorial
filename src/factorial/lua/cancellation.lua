local queue_cancelled_key = KEYS[1]
local queue_backoff_key = KEYS[2]
local queue_orphaned_key = KEYS[3]
local pending_cancellations_key = KEYS[4]
local task_statuses_key = KEYS[5]
local task_agents_key = KEYS[6]
local task_payloads_key = KEYS[7]
local task_pickups_key = KEYS[8]
local task_retries_key = KEYS[9]
local task_metas_key = KEYS[10]
local pending_tool_results_key = KEYS[11]
local agent_metrics_bucket_key = KEYS[12]
local global_metrics_bucket_key = KEYS[13]

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
    return { false, nil, "Task not found" }
elseif task_result.state == "corrupted" then
    return { false, task_result.status, "Task data is corrupted" }
elseif status == "completed" or status == "failed" or status == "cancelled" then
    return { false, status, "Task already in terminal state" }
end

local meta_json = task_result.meta
local pickups = task_result.pickups
local retries = task_result.retries

if status == "pending_tool_results" then
    -- Delete the pending tool results (DEL is safe even if key doesn't exist)
    redis.call('DEL', pending_tool_results_key)
    -- Update task status to cancelled
    redis.call('HSET', task_statuses_key, task_id, 'cancelled')
    -- Add to cancelled queue
    redis.call('ZADD', queue_cancelled_key, timestamp, task_id)

    -- Update timeline metrics
    inc_metrics(
        { agent_metrics_bucket_key, global_metrics_bucket_key },
        { 'cancelled', pickups, retries, meta_json, metrics_ttl }
    )

    return { true, status, "Task cancelled" }
elseif status == "backoff" then
    -- Task is in backoff queue - cancel immediately
    -- Remove from backoff queue (ZREM is safe even if task doesn't exist in the set)
    redis.call('ZREM', queue_backoff_key, task_id)
    -- Update task status to cancelled
    redis.call('HSET', task_statuses_key, task_id, 'cancelled')
    -- Add to cancelled queue
    redis.call('ZADD', queue_cancelled_key, timestamp, task_id)
    -- Update timeline metrics
    inc_metrics(
        { agent_metrics_bucket_key, global_metrics_bucket_key },
        { 'cancelled', pickups, retries, meta_json, metrics_ttl }
    )

    return { true, status, "Task cancelled" }
else
    -- Task is in queued, active, or processing state
    -- Add to cancellation set for worker to handle
    redis.call('SADD', pending_cancellations_key, task_id)
    return { true, status, "Task added to cancellation set" }
end
