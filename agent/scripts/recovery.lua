local task_data_key_template = KEYS[1]
local queue_main_key = KEYS[2]
local processing_heartbeats_key = KEYS[3]
local queue_failed_key = KEYS[4]
local task_metrics_key_template = KEYS[5]
local cutoff_timestamp = tonumber(ARGV[1])
local max_recovery_batch = tonumber(ARGV[2] or 100)
local max_retries = tonumber(ARGV[3])
local agent_name = ARGV[4]
local metrics_bucket_duration = tonumber(ARGV[5])
local metrics_retention_duration = tonumber(ARGV[6])

-- Helper function to update timeline metrics
local function update_timeline_metrics(metric_type, task_duration, task_pickups, task_retries)
    local task_turns = task_pickups - task_retries

    -- Calculate bucket timestamp
    local time_result = redis.call('TIME')
    local timestamp = tonumber(time_result[1]) + (tonumber(time_result[2]) / 1000000)
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
local function get_task_metadata(task_data_key, curr_timestamp)
    local task_data = redis.call('HMGET', task_data_key, 'created_at', 'pickups', 'retries')
    local created_at = tonumber(task_data[1])
    local pickups = tonumber(task_data[2]) or 0
    local retries = tonumber(task_data[3]) or 0

    local duration = 0
    if created_at then
        duration = curr_timestamp - created_at
    end

    return duration, pickups, retries
end

-- Get stale processing keys (limited batch to avoid long-running operations)
local stale_task_ids = redis.call('ZRANGEBYSCORE', processing_heartbeats_key, 0, cutoff_timestamp, 'LIMIT', 0,
    max_recovery_batch)

local recovered_count = 0
local failed_count = 0
local time_result = redis.call('TIME')
local curr_timestamp = tonumber(time_result[1]) + (tonumber(time_result[2]) / 1000000)
local stale_task_actions = {}

for i = 1, #stale_task_ids do
    local task_id = stale_task_ids[i]
    local task_data_key = string.gsub(task_data_key_template, '{task_id}', task_id)

    -- Check if task data exists first
    local task_exists = redis.call('EXISTS', task_data_key)

    redis.call('ZREM', processing_heartbeats_key, task_id)

    if task_exists == 0 then
        -- Task data doesn't exist, just remove from processing and ignore
        table.insert(stale_task_actions, { task_id, "ignored" })
    else
        local task_retries_str = redis.call('HGET', task_data_key, "retries")
        local task_retries = tonumber(task_retries_str) or -1

        local task_status = redis.call('HGET', task_data_key, "status")
        if task_status ~= "processing" then
            -- orphaned heartbeat left task in processing set but has been continued/completed/failed/cancelled
            table.insert(stale_task_actions, { task_id, "ignored" })
        else
            -- Check if task exists and route based on retry count
            if task_retries >= 0 then
                if task_retries < max_retries then
                    -- Increment retry count and put back in queue and set status to active
                    redis.call('HINCRBY', task_data_key, "retries", 1)
                    redis.call('HSET', task_data_key, "status", "active")
                    redis.call('LPUSH', queue_main_key, task_id)
                    table.insert(stale_task_actions, { task_id, "recovered" })
                    recovered_count = recovered_count + 1

                    -- Update timeline metrics for retry
                    local duration, pickups, retries = get_task_metadata(task_data_key, curr_timestamp)
                    update_timeline_metrics("retried", duration, pickups, retries)
                else
                    -- Max retries exceeded, send to failed queue and set status to failed
                    redis.call('ZADD', queue_failed_key, curr_timestamp, task_id)
                    redis.call('HSET', task_data_key, "status", "failed")
                    table.insert(stale_task_actions, { task_id, "failed" })
                    failed_count = failed_count + 1

                    -- Update timeline metrics for failure
                    local duration, pickups, retries = get_task_metadata(task_data_key, curr_timestamp)
                    update_timeline_metrics("failed", duration, pickups, retries)
                end
            end
        end
    end
end

return { recovered_count, failed_count, stale_task_actions }
