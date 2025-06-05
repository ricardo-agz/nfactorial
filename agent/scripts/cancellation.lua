local task_data_key = KEYS[1]
local task_cancellations_key = KEYS[2]
local queue_key_base = KEYS[3]
local pending_tool_results_key_base = KEYS[4]
local task_metrics_key_base = KEYS[5]

local task_id = ARGV[1]
local metrics_bucket_duration = tonumber(ARGV[2])
local metrics_retention_duration = tonumber(ARGV[3])

local time_result = redis.call('TIME')
local timestamp = tonumber(time_result[1]) + (tonumber(time_result[2]) / 1000000)

-- Helper function to update timeline metrics for cancelled tasks
local function update_cancelled_timeline_metrics(created_at, pickups, retries, agent_name)
    local duration = 0
    local task_turns = pickups - retries

    if created_at then
        duration = timestamp - created_at
    end

    -- Calculate bucket timestamp
    local bucket_timestamp = math.floor(timestamp / metrics_bucket_duration) * metrics_bucket_duration

    local agent_metrics_key = task_metrics_key_base .. ":" .. agent_name .. ":cancelled:" .. bucket_timestamp
    local all_metrics_key = task_metrics_key_base .. ":__all__:cancelled:" .. bucket_timestamp

    -- Update agent-specific metrics
    redis.call('HINCRBY', agent_metrics_key, 'count', 1)
    redis.call('HINCRBYFLOAT', agent_metrics_key, 'total_duration', duration)
    redis.call('HINCRBY', agent_metrics_key, 'total_turns', task_turns)
    redis.call('HINCRBY', agent_metrics_key, 'total_retries', retries)
    redis.call('HINCRBY', agent_metrics_key, 'total_pickups', pickups)
    redis.call('EXPIRE', agent_metrics_key, metrics_retention_duration)

    -- Update global metrics
    redis.call('HINCRBY', all_metrics_key, 'count', 1)
    redis.call('HINCRBYFLOAT', all_metrics_key, 'total_duration', duration)
    redis.call('HINCRBY', all_metrics_key, 'total_turns', task_turns)
    redis.call('HINCRBY', all_metrics_key, 'total_retries', retries)
    redis.call('HINCRBY', all_metrics_key, 'total_pickups', pickups)
    redis.call('EXPIRE', all_metrics_key, metrics_retention_duration)
end

-- Get current task status to check if task exists and is in a terminal state
local task_data = redis.call('HMGET', task_data_key, 'created_at', 'status', 'pickups', 'retries', 'agent')
local created_at = tonumber(task_data[1])
local status = task_data[2]
local pickups = tonumber(task_data[3]) or 0
local retries = tonumber(task_data[4]) or 0
local agent_name = task_data[5]

if status == nil then
    return { false, nil, "Task not found" }
elseif status == "completed" or status == "failed" or status == "cancelled" then
    return { false, status, "Task already in terminal state" }
end

local queue_backoff_key = queue_key_base .. agent_name .. ":backoff"
local queue_cancelled_key = queue_key_base .. agent_name .. ":cancelled"

-- Handle different cancellation scenarios based on task status
if status == "pending_tool_results" then
    -- Task is waiting for tool results - cancel immediately
    local pending_tool_results_key = pending_tool_results_key_base .. ":" .. task_id

    -- Delete the pending tool results (DEL is safe even if key doesn't exist)
    redis.call('DEL', pending_tool_results_key)

    -- Update task status to cancelled
    redis.call('HSET', task_data_key, 'status', 'cancelled')

    -- Add to cancelled queue
    redis.call('ZADD', queue_cancelled_key, timestamp, task_id)

    -- Update metrics
    update_cancelled_timeline_metrics(created_at, pickups, retries, agent_name)

    return { true, status, "Task cancelled" }
elseif status == "backoff" then
    -- Task is in backoff queue - cancel immediately

    -- Remove from backoff queue (ZREM is safe even if task doesn't exist in the set)
    redis.call('ZREM', queue_backoff_key, task_id)

    -- Update task status to cancelled
    redis.call('HSET', task_data_key, 'status', 'cancelled')

    -- Add to cancelled queue
    redis.call('ZADD', queue_cancelled_key, timestamp, task_id)

    -- Update metrics
    update_cancelled_timeline_metrics(created_at, pickups, retries, agent_name)

    return { true, status, "Task cancelled" }
else
    -- Task is in queued, active, or processing state
    -- Add to cancellation set for worker to handle
    redis.call('SADD', task_cancellations_key, task_id)
    return { true, status, "Task added to cancellation set" }
end
