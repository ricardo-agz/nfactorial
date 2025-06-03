local tasks_data_key = KEYS[1]
local processing_heartbeats_key = KEYS[2]
local queue_main_key = KEYS[3]
local queue_completions_key = KEYS[4]
local queue_failed_key = KEYS[5]
local pending_tool_results_key = KEYS[6]

local task_id = ARGV[1]
local action = ARGV[2]  -- "complete", "continue", "retry", or "fail"
local updated_task_payload_json = ARGV[3]  -- Only used for continue/fail
-- Optional parameters - may not be provided
local pending_tool_call_result_sentinel = ARGV[4]
local pending_tool_call_ids_json = ARGV[5]

local time_result = redis.call('TIME')
local timestamp = tonumber(time_result[1]) + (tonumber(time_result[2]) / 1000000)

-- Verify the task exists in processing heartbeats
local task_exists = redis.call('ZSCORE', processing_heartbeats_key, task_id)
if task_exists == false then
    return false
end

-- Always remove from processing first
redis.call('ZREM', processing_heartbeats_key, task_id)

-- Handle pending tool call results only if both parameters are provided
if pending_tool_call_ids_json and pending_tool_call_result_sentinel then
    
    local pending_tool_call_ids = cjson.decode(pending_tool_call_ids_json)
    for _, tool_call_id in ipairs(pending_tool_call_ids) do
        redis.call('HSET', pending_tool_results_key, tool_call_id, pending_tool_call_result_sentinel)
    end
    -- Set task status to pending tool call results
    redis.call('HSET', tasks_data_key, "status", "pending_tool_results")
    return true
end

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
