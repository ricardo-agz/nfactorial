local tasks_data_key = KEYS[1]
local processing_heartbeats_key = KEYS[2]
local queue_main_key = KEYS[3]
local queue_completions_key = KEYS[4]
local queue_failed_key = KEYS[5]

local task_id = ARGV[1]
local action = ARGV[2]  -- "complete", "continue", "retry", or "fail"
local updated_task_payload_json = ARGV[3]  -- Only used for continue/fail
local timestamp = redis.call('TIME')

-- Verify the task exists in processing heartbeats
local task_exists = redis.call('ZSCORE', processing_heartbeats_key, task_id)
if task_exists == false then
    return false
end

-- Always remove from processing first
redis.call('ZREM', processing_heartbeats_key, task_id)

if action == "complete" then
    -- Task finished successfully, add to completions queue
    redis.call('ZADD', queue_completions_key, timestamp, task_id)
    -- Set task status to completed
    redis.call('HSET', tasks_data_key, "status", "completed")
    return true
    
elseif action == "continue" then
    -- Task needs more processing, put back at front of main queue
    redis.call('LPUSH', queue_main_key, task_id)
    -- Update the task status and payload
    redis.call('HSET', tasks_data_key, "status", "active")
    redis.call('HSET', tasks_data_key, "payload", updated_task_payload_json)
    return true

elseif action == "retry" then
    -- Requeue and update the task status and retry count
    redis.call('LPUSH', queue_main_key, task_id)
    redis.call('HSET', tasks_data_key, "status", "active")
    redis.call('HINCRBY', tasks_data_key, "retries", 1)

    return true
    
elseif action == "fail" then
    -- Add to failed queue and set task status to failed
    redis.call('ZADD', queue_failed_key, timestamp, task_id)
    redis.call('HSET', tasks_data_key, "status", "failed")
    return true

else
    error("invalid_action")
end 
