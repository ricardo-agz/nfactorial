local queue_main_key = KEYS[1]
local queue_completions_key = KEYS[2]
local queue_failed_key = KEYS[3]
local queue_backoff_key = KEYS[4]
local queue_orphaned_key = KEYS[5]
local task_statuses_key = KEYS[6]
local task_agents_key = KEYS[7]
local task_payloads_key = KEYS[8]
local task_pickups_key = KEYS[9]
local task_retries_key = KEYS[10]
local task_metas_key = KEYS[11]
local processing_heartbeats_key = KEYS[12]
local pending_tool_results_key = KEYS[13]
local agent_metrics_bucket_key = KEYS[14]
local global_metrics_bucket_key = KEYS[15]
local agent_idle_gauge_key = KEYS[16]
local global_idle_gauge_key = KEYS[17]

local task_id = ARGV[1]
local action = ARGV[2]                    -- complete, continue, retry, fail, pending
local updated_task_payload_json = ARGV[3] -- Only used for continue/fail
local metrics_ttl = tonumber(ARGV[4])
-- Optional parameters - may not be provided
local pending_sentinel = ARGV[5]
local pending_tool_call_ids_json = ARGV[6]

local time_result = redis.call('TIME')
local timestamp = tonumber(time_result[1]) + (tonumber(time_result[2]) / 1000000)

-- Always remove from processing first
redis.call('ZREM', processing_heartbeats_key, task_id)

local task_result = load_task(
    { task_statuses_key, task_agents_key, task_payloads_key, task_pickups_key, task_retries_key, task_metas_key },
    { task_id }
)
if task_result.state == "missing" then
    -- Task is missing, add to orphaned queue
    redis.call('ZADD', queue_orphaned_key, timestamp, task_id)
    return false
elseif task_result.state == "corrupted" then
    -- Task data is corrupted, skip it and log it
    return false
end

local status = task_result.status
local meta_json = task_result.meta
local pickups = task_result.pickups
local retries = task_result.retries
local meta = cjson.decode(meta_json)

-- Handle pending tool call results only if both parameters are provided
if pending_tool_call_ids_json and pending_sentinel then
    local pending_tool_call_ids = cjson.decode(pending_tool_call_ids_json)
    for _, tool_call_id in ipairs(pending_tool_call_ids) do
        redis.call('HSET', pending_tool_results_key, tool_call_id, pending_sentinel)
    end
    -- Set task status to pending tool call results
    redis.call('HSET', task_statuses_key, task_id, "pending_tool_results")

    -- Update idle gauge
    redis.call('INCR', agent_idle_gauge_key)
    redis.call('INCR', global_idle_gauge_key)

    return true
end

if action == "complete" then
    -- Check if task is in a valid state for completion
    if status ~= "processing" and status ~= "pending_tool_results" then
        return false
    end

    -- Task finished successfully, add to completions queue
    redis.call('ZADD', queue_completions_key, timestamp, task_id)
    -- Set task status to completed
    redis.call('HSET', task_statuses_key, task_id, "completed")

    -- Update timeline metrics
    inc_metrics(
        { agent_metrics_bucket_key, global_metrics_bucket_key },
        { 'completed', pickups, retries, meta_json, metrics_ttl }
    )

    return true
elseif action == "continue" then
    -- Check if task is in a valid state for continuation
    if status ~= "processing" then
        return false
    end

    -- Task needs more processing, put back at front of main queue
    redis.call('LPUSH', queue_main_key, task_id)
    -- Update the task status and payload
    redis.call('HSET', task_statuses_key, task_id, "active")
    redis.call('HSET', task_payloads_key, task_id, updated_task_payload_json)
    return true
elseif action == "retry" then
    -- Check if task is in a valid state for retry
    if status ~= "processing" then
        return false
    end

    -- Requeue and update the task status and retry count
    redis.call('LPUSH', queue_main_key, task_id)
    redis.call('HSET', task_statuses_key, task_id, "active")
    redis.call('HINCRBY', task_retries_key, task_id, 1)
    redis.call('HSET', task_metas_key, task_id, cjson.encode(meta))

    -- Update timeline metrics for retry
    inc_metrics(
        { agent_metrics_bucket_key, global_metrics_bucket_key },
        { 'retried', pickups, retries, meta_json, metrics_ttl }
    )

    return true
elseif action == "backoff" then
    -- Check if task is in a valid state for backoff
    if status ~= "processing" then
        return false
    end

    -- Add to backoff queue with exponential backoff
    local backoff_duration = math.min(5 * 2 ^ retries, 300) -- Cap at 5 minutes
    redis.call('ZADD', queue_backoff_key, timestamp + backoff_duration, task_id)
    redis.call('HSET', task_statuses_key, task_id, "backoff")
    redis.call('HINCRBY', task_retries_key, task_id, 1)
    redis.call('HSET', task_metas_key, task_id, cjson.encode(meta))

    return true
elseif action == "fail" then
    -- Check if task is in a valid state for failure
    if status ~= "processing" and status ~= "pending_tool_results" then
        return false
    end

    -- Add to failed queue and set task status to failed
    redis.call('ZADD', queue_failed_key, timestamp, task_id)
    redis.call('HSET', task_statuses_key, task_id, "failed")

    -- Update timeline metrics
    inc_metrics(
        { agent_metrics_bucket_key, global_metrics_bucket_key },
        { 'failed', pickups, retries, meta_json, metrics_ttl }
    )

    return true
else
    error("invalid_action")
end
