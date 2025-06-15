local queue_completions_key = KEYS[1]
local queue_failed_key = KEYS[2]
local queue_cancelled_key = KEYS[3]
local queue_orphaned_key = KEYS[4]
local task_statuses_key = KEYS[5]
local task_agents_key = KEYS[6]
local task_payloads_key = KEYS[7]
local task_pickups_key = KEYS[8]
local task_retries_key = KEYS[9]
local task_metas_key = KEYS[10]

local completed_cutoff_timestamp = tonumber(ARGV[1])
local failed_cutoff_timestamp = tonumber(ARGV[2])
local cancelled_cutoff_timestamp = tonumber(ARGV[3])
local max_cleanup_batch = tonumber(ARGV[4])

local time_result = redis.call('TIME')
local timestamp = tonumber(time_result[1]) + (tonumber(time_result[2]) / 1000000)

-- Input validation
if not max_cleanup_batch or max_cleanup_batch <= 0 then
    error("max_cleanup_batch must be greater than 0")
end

-- Early exit if no cutoff timestamps are provided
if not completed_cutoff_timestamp or not failed_cutoff_timestamp or not cancelled_cutoff_timestamp then
    error("all cutoff timestamps must be provided")
end

local completed_cleaned = 0
local failed_cleaned = 0
local cancelled_cleaned = 0
local cleaned_task_ids = {}

-- Helper function to clean expired tasks from a queue
local function clean_expired_tasks(queue_key, cutoff_timestamp, max_batch)
    if not queue_key or not cutoff_timestamp or not max_batch or max_batch <= 0 then
        return 0, {}
    end

    -- Get tasks older than cutoff timestamp, limited by max_batch
    local expired_tasks = redis.call('ZRANGEBYSCORE', queue_key, '-inf', cutoff_timestamp, 'LIMIT', 0, max_batch)

    if #expired_tasks == 0 then
        return 0, {}
    end

    for i = 1, #expired_tasks do
        redis.call('ZREM', queue_key, expired_tasks[i])
    end

    local task_ids = {}
    for i = 1, #expired_tasks do
        local task_id = expired_tasks[i]

        local task_result = load_task(
            { task_statuses_key, task_agents_key, task_payloads_key, task_pickups_key, task_retries_key, task_metas_key },
            { task_id }
        )

        if task_result.state == "missing" then
            -- Task is missing, add to orphaned queue
            redis.call('ZADD', queue_orphaned_key, timestamp, task_id)
        else
            redis.call('HDEL', task_statuses_key, task_id)
            redis.call('HDEL', task_agents_key, task_id)
            redis.call('HDEL', task_payloads_key, task_id)
            redis.call('HDEL', task_pickups_key, task_id)
            redis.call('HDEL', task_retries_key, task_id)
            redis.call('HDEL', task_metas_key, task_id)
        end

        table.insert(task_ids, task_id)
    end

    return #expired_tasks, task_ids
end

-- Clean completed tasks
local completed_count, completed_ids = clean_expired_tasks(queue_completions_key, completed_cutoff_timestamp,
    max_cleanup_batch)
completed_cleaned = completed_count
for i = 1, #completed_ids do
    table.insert(cleaned_task_ids, { 'completed', completed_ids[i] })
end

-- Clean failed tasks
local failed_count, failed_ids = clean_expired_tasks(queue_failed_key, failed_cutoff_timestamp, max_cleanup_batch)
failed_cleaned = failed_count
for i = 1, #failed_ids do
    table.insert(cleaned_task_ids, { 'failed', failed_ids[i] })
end

-- Clean cancelled tasks
local cancelled_count, cancelled_ids = clean_expired_tasks(queue_cancelled_key, cancelled_cutoff_timestamp,
    max_cleanup_batch)
cancelled_cleaned = cancelled_count
for i = 1, #cancelled_ids do
    table.insert(cleaned_task_ids, { 'cancelled', cancelled_ids[i] })
end

return { completed_cleaned, failed_cleaned, cancelled_cleaned, cleaned_task_ids }
