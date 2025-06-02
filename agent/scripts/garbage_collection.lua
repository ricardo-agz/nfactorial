--[[
Garbage Collection Script for TTL-based task cleanup

This script removes expired tasks from completion, failed, and cancelled queues
and deletes their associated task data.

Keys:
* KEYS[1] = task_data_key_template (str) - template for task data keys
* KEYS[2] = queue_completions_key (str) - completed tasks queue
* KEYS[3] = queue_failed_key (str) - failed tasks queue  
* KEYS[4] = queue_cancelled_key (str) - cancelled tasks queue

Args:
* ARGV[1] = completed_cutoff_timestamp (float) - tasks completed before this are expired
* ARGV[2] = failed_cutoff_timestamp (float) - tasks failed before this are expired
* ARGV[3] = cancelled_cutoff_timestamp (float) - tasks cancelled before this are expired
* ARGV[4] = max_cleanup_batch (int) - maximum tasks to clean up per queue per run

Returns:
* [completed_cleaned, failed_cleaned, cancelled_cleaned, cleaned_task_ids]
--]]

local task_data_key_template = KEYS[1]
local queue_completions_key = KEYS[2]
local queue_failed_key = KEYS[3]
local queue_cancelled_key = KEYS[4]

local completed_cutoff_timestamp = tonumber(ARGV[1])
local failed_cutoff_timestamp = tonumber(ARGV[2])  
local cancelled_cutoff_timestamp = tonumber(ARGV[3])
local max_cleanup_batch = tonumber(ARGV[4])

-- Input validation
if not max_cleanup_batch or max_cleanup_batch <= 0 then
    return {0, 0, 0, {}}
end

-- Early exit if no cutoff timestamps are provided
if not completed_cutoff_timestamp and not failed_cutoff_timestamp and not cancelled_cutoff_timestamp then
    return {0, 0, 0, {}}
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
    
    -- Prepare task data keys for batch deletion
    local task_data_keys = {}
    local task_ids = {}
    for i = 1, #expired_tasks do
        local task_id = expired_tasks[i]
        local task_data_key = string.gsub(task_data_key_template, '{task_id}', task_id)
        table.insert(task_data_keys, task_data_key)
        table.insert(task_ids, task_id)
    end
    
    -- Batch delete all task data
    if #task_data_keys > 0 then
        redis.call('DEL', unpack(task_data_keys))
    end
    
    return #expired_tasks, task_ids
end

-- Clean completed tasks
local completed_count, completed_ids = clean_expired_tasks(queue_completions_key, completed_cutoff_timestamp, max_cleanup_batch)
completed_cleaned = completed_count
for i = 1, #completed_ids do
    table.insert(cleaned_task_ids, {'completed', completed_ids[i]})
end

-- Clean failed tasks  
local failed_count, failed_ids = clean_expired_tasks(queue_failed_key, failed_cutoff_timestamp, max_cleanup_batch)
failed_cleaned = failed_count
for i = 1, #failed_ids do
    table.insert(cleaned_task_ids, {'failed', failed_ids[i]})
end

-- Clean cancelled tasks
local cancelled_count, cancelled_ids = clean_expired_tasks(queue_cancelled_key, cancelled_cutoff_timestamp, max_cleanup_batch)  
cancelled_cleaned = cancelled_count
for i = 1, #cancelled_ids do
    table.insert(cleaned_task_ids, {'cancelled', cancelled_ids[i]})
end

return {completed_cleaned, failed_cleaned, cancelled_cleaned, cleaned_task_ids} 