local task_data_key_template = KEYS[1]
local queue_main_key = KEYS[2]
local queue_cancelled_key = KEYS[3]
local processing_heartbeats_key = KEYS[4]
local task_cancellations_key = KEYS[5]
local queue_orphaned_key = KEYS[6]
local task_metrics_key_template = KEYS[7]

local batch_size = tonumber(ARGV[1])
local agent_name = ARGV[2]
-- Metrics configuration parameters
local metrics_bucket_duration = tonumber(ARGV[3])    -- in seconds
local metrics_retention_duration = tonumber(ARGV[4]) -- in seconds

local time_result = redis.call('TIME')
local timestamp = tonumber(time_result[1]) + (tonumber(time_result[2]) / 1000000)

-- Helper function to update timeline metrics for cancelled tasks
local function update_cancelled_timeline_metrics(task_data_key)
    -- Get task metadata for metrics
    local task_data = redis.call('HMGET', task_data_key, 'created_at', 'pickups', 'retries')
    local created_at = tonumber(task_data[1])
    local pickups = tonumber(task_data[2]) or 0
    local retries = tonumber(task_data[3]) or 0

    local duration = 0
    local task_turns = pickups - retries

    if created_at then
        duration = timestamp - created_at
    end

    -- Calculate bucket timestamp
    local bucket_timestamp = math.floor(timestamp / metrics_bucket_duration) * metrics_bucket_duration

    -- "timeline:{agent}:cancelled:{bucket}"
    local agent_metrics_key = string.gsub(task_metrics_key_template, "{agent}", agent_name)
    agent_metrics_key = string.gsub(agent_metrics_key, "{metric_type}", "cancelled")
    agent_metrics_key = string.gsub(agent_metrics_key, "{bucket}", tostring(bucket_timestamp))

    local all_metrics_key = string.gsub(task_metrics_key_template, "{agent}", "__all__")
    all_metrics_key = string.gsub(all_metrics_key, "{metric_type}", "cancelled")
    all_metrics_key = string.gsub(all_metrics_key, "{bucket}", tostring(bucket_timestamp))

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

local tasks_to_process_ids = {}
local tasks_to_cancel_ids = {}
local orphaned_task_ids = {}

-- Try to get more tasks than requested to account for cancelled ones
local attempts = batch_size * 2

for i = 1, attempts do
    local task_id = redis.call('LPOP', queue_main_key)
    if not task_id then
        break
    end

    local task_data_key = string.gsub(task_data_key_template, '{task_id}', task_id)

    -- Check if cancelled
    if redis.call('SISMEMBER', task_cancellations_key, task_id) == 1 then
        -- Remove from cancelled set
        redis.call('SREM', task_cancellations_key, task_id)
        redis.call('ZADD', queue_cancelled_key, timestamp, task_id)
        -- Update the task status to cancelled
        redis.call('HSET', task_data_key, 'status', 'cancelled')

        -- Update timeline metrics for cancelled task
        update_cancelled_timeline_metrics(task_data_key)

        table.insert(tasks_to_cancel_ids, task_id)
    else
        -- Check if task data exists before updating it (safety check)
        local task_exists = redis.call('HEXISTS', task_data_key, 'id')
        if task_exists == 1 then
            -- Add task_id to processing and set heartbeat to current timestamp
            redis.call('ZADD', processing_heartbeats_key, timestamp, task_id)
            -- Update the task status to processing
            redis.call('HSET', task_data_key, 'status', 'processing')
            redis.call('HINCRBY', task_data_key, 'pickups', 1)
            table.insert(tasks_to_process_ids, task_id)

            if #tasks_to_process_ids >= batch_size then
                break
            end
        else
            -- Add to orphaned tasks set with timestamp for investigation
            redis.call('ZADD', queue_orphaned_key, timestamp, task_id)
            table.insert(orphaned_task_ids, task_id)
        end
    end
end

return { tasks_to_process_ids, tasks_to_cancel_ids, orphaned_task_ids }
