local task_data_key_template = KEYS[1]
local queue_main_key = KEYS[2]
local processing_heartbeats_key = KEYS[3] 
local queue_failed_key = KEYS[4]
local cutoff_timestamp = tonumber(ARGV[1])
local max_recovery_batch = tonumber(ARGV[2] or 100)
local max_retries = tonumber(ARGV[3])

-- Get stale processing keys (limited batch to avoid long-running operations)
local stale_task_ids = redis.call('ZRANGEBYSCORE', processing_heartbeats_key, 0, cutoff_timestamp, 'LIMIT', 0, max_recovery_batch)

local recovered_count = 0
local failed_count = 0
local curr_timestamp = redis.call('TIME')
local stale_task_actions = {}

for i = 1, #stale_task_ids do
    local task_id = stale_task_ids[i]
    local task_data_key = string.gsub(task_data_key_template, '{task_id}', task_id)
    
    local task_retries_str = redis.call('HGET', task_data_key, "retries")
    local task_retries = tonumber(task_retries_str) or -1

    redis.call('ZREM', processing_heartbeats_key, task_id)

    -- Check if task exists and route based on retry count
    if task_retries >= 0 then
        if task_retries < max_retries then
            -- Increment retry count and put back in queue and set status to active
            redis.call('HSET', task_data_key, "retries", task_retries + 1)
            redis.call('HSET', task_data_key, "status", "active")
            redis.call('LPUSH', queue_main_key, task_id)
            table.insert(stale_task_actions, {task_id, "recovered"})
            recovered_count = recovered_count + 1
        else
            -- Max retries exceeded, send to failed queue and set status to failed
            redis.call('ZADD', queue_failed_key, curr_timestamp[1], task_id)
            redis.call('HSET', task_data_key, "status", "failed")
            table.insert(stale_task_actions, {task_id, "failed"})
            failed_count = failed_count + 1
        end
    end
end

return {recovered_count, failed_count, stale_task_actions}
