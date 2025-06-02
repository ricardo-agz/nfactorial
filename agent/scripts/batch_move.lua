local task_data_key_template = KEYS[1]
local queue_main_key = KEYS[2]
local queue_cancelled_key = KEYS[3]
local processing_heartbeats_key = KEYS[4]
local task_cancellations_key = KEYS[5]
local batch_size = tonumber(ARGV[1])
local timestamp = redis.call('TIME')

local tasks_to_process_ids = {}
local tasks_to_cancel_ids = {}

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
        redis.call('ZADD', queue_cancelled_key, timestamp[1], task_id)
        -- Update the task status to cancelled
        redis.call('HSET', task_data_key, 'status', 'cancelled')

        table.insert(tasks_to_cancel_ids, task_id)
    else
        -- Add task_id to processing and set heartbeat to current timestamp
        redis.call('ZADD', processing_heartbeats_key, timestamp[1], task_id)
        -- Update the task status to processing
        redis.call('HSET', task_data_key, 'status', 'processing')
        table.insert(tasks_to_process_ids, task_id)

        if #tasks_to_process_ids >= batch_size then
            break
        end
    end
end

return {tasks_to_process_ids, tasks_to_cancel_ids}
