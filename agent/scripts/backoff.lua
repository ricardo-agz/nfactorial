local queue_backoff_key = KEYS[1]
local queue_main_key = KEYS[2]
local task_data_key_template = KEYS[3]
local max_batch_size = tonumber(ARGV[1])

local time_result = redis.call('TIME')
local current_timestamp = tonumber(time_result[1]) + (tonumber(time_result[2]) / 1000000)

-- Get ready tasks (score <= current_timestamp)
local ready_task_ids = redis.call('ZRANGEBYSCORE', queue_backoff_key, 0, current_timestamp, 'LIMIT', 0, max_batch_size)

if #ready_task_ids == 0 then
    return {}
end

-- Always remove from backoff queue in batch
redis.call('ZREM', queue_backoff_key, unpack(ready_task_ids))

-- Batch get all task statuses in one call
local task_data_keys = {}
for i = 1, #ready_task_ids do
    local task_id = ready_task_ids[i]
    local task_data_key = string.gsub(task_data_key_template, '{task_id}', task_id)
    table.insert(task_data_keys, task_data_key)
end

-- Get all statuses at once using MGET with HGET simulation
local task_statuses = {}
for i = 1, #task_data_keys do
    task_statuses[i] = redis.call('HGET', task_data_keys[i], 'status')
end

local recovered_tasks = {}

for i = 1, #ready_task_ids do
    local task_id = ready_task_ids[i]
    local task_status = task_statuses[i]

    if task_status == 'backoff' then
        table.insert(recovered_tasks, task_id)
    end
    -- Else task doesn't exist or status changed - cleanup handled by always removing from backoff queue
end

-- Batch execute recovery operations
if #recovered_tasks > 0 then
    -- Update statuses in batch
    for i = 1, #recovered_tasks do
        local task_id = recovered_tasks[i]
        local task_data_key = string.gsub(task_data_key_template, '{task_id}', task_id)
        redis.call('HSET', task_data_key, 'status', 'active')
    end

    -- Push to main queue in batch
    redis.call('LPUSH', queue_main_key, unpack(recovered_tasks))
end

return recovered_tasks
