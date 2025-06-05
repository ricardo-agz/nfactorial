local tasks_data_key = KEYS[1]
local processing_heartbeats_key = KEYS[2]
local queue_main_key = KEYS[3]
local queue_completions_key = KEYS[4]
local queue_failed_key = KEYS[5]
local pending_tool_results_key = KEYS[6]
local queue_backoff_key = KEYS[7]
local task_metrics_key_template = KEYS[8]
local gauge_idle_key_template = KEYS[9]

local task_id = ARGV[1]
local agent_name = ARGV[2]
local action = ARGV[3]                               -- complete, continue, retry, fail, pending
local updated_task_payload_json = ARGV[4]            -- Only used for continue/fail
-- Metrics configuration parameters
local metrics_bucket_duration = tonumber(ARGV[5])    -- in seconds
local metrics_retention_duration = tonumber(ARGV[6]) -- in seconds
-- Optional parameters - may not be provided
local pending_tool_call_result_sentinel = ARGV[7]
local pending_tool_call_ids_json = ARGV[8]

local time_result = redis.call('TIME')
local timestamp = tonumber(time_result[1]) + (tonumber(time_result[2]) / 1000000)

-- Helper function to update timeline metrics
local function update_timeline_metrics(metric_type, task_duration, task_pickups, task_retries)
    local task_turns = task_pickups - task_retries

    -- Calculate bucket timestamp
    local bucket_timestamp = math.floor(timestamp / metrics_bucket_duration) * metrics_bucket_duration

    -- "timeline:{agent}:{metric_type}:{bucket}"
    local agent_metrics_key = string.gsub(task_metrics_key_template, "{agent}", agent_name)
    agent_metrics_key = string.gsub(agent_metrics_key, "{metric_type}", metric_type)
    agent_metrics_key = string.gsub(agent_metrics_key, "{bucket}", tostring(bucket_timestamp))

    local all_metrics_key = string.gsub(task_metrics_key_template, "{agent}", "__all__")
    all_metrics_key = string.gsub(all_metrics_key, "{metric_type}", metric_type)
    all_metrics_key = string.gsub(all_metrics_key, "{bucket}", tostring(bucket_timestamp))

    -- Update agent-specific metrics
    redis.call('HINCRBY', agent_metrics_key, 'count', 1)
    redis.call('HINCRBYFLOAT', agent_metrics_key, 'total_duration', task_duration)
    redis.call('HINCRBY', agent_metrics_key, 'total_turns', task_turns)
    redis.call('HINCRBY', agent_metrics_key, 'total_retries', task_retries)
    redis.call('HINCRBY', agent_metrics_key, 'total_pickups', task_pickups)
    redis.call('EXPIRE', agent_metrics_key, metrics_retention_duration)

    -- Update global metrics
    redis.call('HINCRBY', all_metrics_key, 'count', 1)
    redis.call('HINCRBYFLOAT', all_metrics_key, 'total_duration', task_duration)
    redis.call('HINCRBY', all_metrics_key, 'total_turns', task_turns)
    redis.call('HINCRBY', all_metrics_key, 'total_retries', task_retries)
    redis.call('HINCRBY', all_metrics_key, 'total_pickups', task_pickups)
    redis.call('EXPIRE', all_metrics_key, metrics_retention_duration)
end

-- Helper function to get task metadata for metrics
local function get_task_metadata()
    local task_data = redis.call('HMGET', tasks_data_key, 'created_at', 'pickups', 'retries')
    local created_at = tonumber(task_data[1])
    local pickups = tonumber(task_data[2]) or 0
    local retries = tonumber(task_data[3]) or 0

    local duration = nil
    local turns = nil

    if created_at then
        duration = timestamp - created_at
    end

    return duration, pickups, retries
end

-- Verify the task exists in processing heartbeats
local task_exists = redis.call('ZSCORE', processing_heartbeats_key, task_id)
if task_exists == false then
    return false
end

-- Always remove from processing first
redis.call('ZREM', processing_heartbeats_key, task_id)

-- Check if task data exists before proceeding with any operations
local task_data_exists = redis.call('EXISTS', tasks_data_key)
if task_data_exists == 0 then
    -- Task data doesn't exist, task has been orphaned
    return false
end

-- Get current task status to check for terminal states
local current_status = redis.call('HGET', tasks_data_key, 'status')

-- Handle pending tool call results only if both parameters are provided
if pending_tool_call_ids_json and pending_tool_call_result_sentinel then
    local pending_tool_call_ids = cjson.decode(pending_tool_call_ids_json)
    for _, tool_call_id in ipairs(pending_tool_call_ids) do
        redis.call('HSET', pending_tool_results_key, tool_call_id, pending_tool_call_result_sentinel)
    end
    -- Set task status to pending tool call results
    redis.call('HSET', tasks_data_key, "status", "pending_tool_results")

    -- Update idle gauge
    local idle_gauge_key = string.gsub(gauge_idle_key_template, "{agent}", agent_name)
    local idle_gauge_key_all = string.gsub(gauge_idle_key_template, "{agent}", "__all__")
    redis.call('INCR', idle_gauge_key)
    redis.call('INCR', idle_gauge_key_all)

    return true
end

if action == "complete" then
    -- Check if task is in a valid state for completion
    if current_status ~= "processing" and current_status ~= "pending_tool_results" then
        return false
    end

    -- Task finished successfully, add to completions queue
    redis.call('ZADD', queue_completions_key, timestamp, task_id)
    -- Set task status to completed
    redis.call('HSET', tasks_data_key, "status", "completed")

    -- Update timeline metrics
    local duration, pickups, retries = get_task_metadata()
    update_timeline_metrics("completed", duration, pickups, retries)

    return true
elseif action == "continue" then
    -- Check if task is in a valid state for continuation
    if current_status ~= "processing" then
        return false
    end

    -- Task needs more processing, put back at front of main queue
    redis.call('LPUSH', queue_main_key, task_id)
    -- Update the task status and payload
    redis.call('HSET', tasks_data_key, "status", "active")
    redis.call('HSET', tasks_data_key, "payload", updated_task_payload_json)
    return true
elseif action == "retry" then
    -- Check if task is in a valid state for retry
    if current_status ~= "processing" then
        return false
    end

    -- Requeue and update the task status and retry count
    redis.call('LPUSH', queue_main_key, task_id)
    redis.call('HSET', tasks_data_key, "status", "active")
    redis.call('HINCRBY', tasks_data_key, "retries", 1)

    -- Update timeline metrics for retry
    local duration, pickups, retries = get_task_metadata()
    update_timeline_metrics("retried", duration, pickups, retries)

    return true
elseif action == "backoff" then
    -- Check if task is in a valid state for backoff
    if current_status ~= "processing" then
        return false
    end

    -- Add to backoff queue with exponential backoff
    local retries = tonumber(redis.call('HGET', tasks_data_key, 'retries')) or 0
    local backoff_duration = math.min(5 * 2 ^ retries, 300) -- Cap at 5 minutes
    redis.call('ZADD', queue_backoff_key, timestamp + backoff_duration, task_id)
    redis.call('HSET', tasks_data_key, "status", "backoff")
    redis.call('HINCRBY', tasks_data_key, "retries", 1)

    return true
elseif action == "fail" then
    -- Check if task is in a valid state for failure
    if current_status ~= "processing" and current_status ~= "pending_tool_results" then
        return false
    end

    -- Add to failed queue and set task status to failed
    redis.call('ZADD', queue_failed_key, timestamp, task_id)
    redis.call('HSET', tasks_data_key, "status", "failed")

    -- Update timeline metrics
    local duration, pickups, retries = get_task_metadata()
    update_timeline_metrics("failed", duration, pickups, retries)

    return true
else
    error("invalid_action")
end
