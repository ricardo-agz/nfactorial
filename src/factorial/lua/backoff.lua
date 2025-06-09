local queue_backoff_key = KEYS[1]
local queue_main_key = KEYS[2]
local queue_orphaned_key = KEYS[3]
local task_statuses_key = KEYS[4]
local task_agents_key = KEYS[5]
local task_payloads_key = KEYS[6]
local task_pickups_key = KEYS[7]
local task_retries_key = KEYS[8]
local task_metas_key = KEYS[9]

local max_batch_size = tonumber(ARGV[1])

local time_result = redis.call('TIME')
local timestamp = tonumber(time_result[1]) + (tonumber(time_result[2]) / 1000000)

-- Get ready tasks (score <= current_timestamp)
local ready_task_ids = redis.call('ZRANGEBYSCORE', queue_backoff_key, 0, timestamp, 'LIMIT', 0, max_batch_size)

if #ready_task_ids == 0 then
    return {}
end

-- Always remove from backoff queue in batch
for i = 1, #ready_task_ids, 1000 do
    local slice = {}
    local slice_end = math.min(i + 999, #ready_task_ids)
    for j = i, slice_end do
        table.insert(slice, ready_task_ids[j])
    end
    redis.call('ZREM', queue_backoff_key, unpack(slice))
end

local recoverable_task_ids = {}
local recoverable_task_statuses = {}

-- Batch get all task statuses in one call
for i = 1, #ready_task_ids do
    local task_id = ready_task_ids[i]

    local task_result = load_task(
        { task_statuses_key, task_agents_key, task_payloads_key, task_pickups_key, task_retries_key, task_metas_key },
        { task_id }
    )

    if task_result.state == "ok" then
        table.insert(recoverable_task_ids, task_id)
        table.insert(recoverable_task_statuses, task_result.status)
    elseif task_result.state == "missing" then
        -- Task is missing, add to orphaned queue
        redis.call('ZADD', queue_orphaned_key, timestamp, task_id)
    end
end

local recovered_tasks = {}

for i = 1, #recoverable_task_ids do
    local task_id = recoverable_task_ids[i]
    local task_status = recoverable_task_statuses[i]

    if task_status == 'backoff' then
        table.insert(recovered_tasks, task_id)
    end
    -- Else task status changed - cleanup handled by always removing from backoff queue
end

-- Batch execute recovery operations
if #recovered_tasks > 0 then
    -- Update statuses in batch
    for i = 1, #recovered_tasks do
        local task_id = recovered_tasks[i]
        redis.call('HSET', task_statuses_key, task_id, 'active')
    end

    -- Push to main queue in batch
    redis.call('LPUSH', queue_main_key, unpack(recovered_tasks))
end

return recovered_tasks
