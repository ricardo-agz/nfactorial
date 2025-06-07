local queue_main_key = KEYS[1]
local queue_cancelled_key = KEYS[2]
local queue_orphaned_key = KEYS[3]
local task_statuses_key = KEYS[4]
local task_agents_key = KEYS[5]
local task_payloads_key = KEYS[6]
local task_pickups_key = KEYS[7]
local task_retries_key = KEYS[8]
local task_metas_key = KEYS[9]
local task_cancellations_key = KEYS[10]
local processing_heartbeats_key = KEYS[11]
local agent_metrics_bucket_key = KEYS[12]
local global_metrics_bucket_key = KEYS[13]

local batch_size = tonumber(ARGV[1])
local metrics_ttl = tonumber(ARGV[2])

local time_result = redis.call('TIME')
local timestamp = tonumber(time_result[1]) + (tonumber(time_result[2]) / 1000000)

local tasks_to_process_ids = {}
local tasks_to_cancel_ids = {}
local orphaned_task_ids = {}
local corrupted_task_ids = {}

-- Try to get more tasks than requested to account for cancelled ones
local attempts = batch_size * 2

for i = 1, attempts do
    local task_id = redis.call('LPOP', queue_main_key)
    if not task_id then
        break
    end

    local task_result = load_task(
        { task_statuses_key, task_agents_key, task_payloads_key, task_pickups_key, task_retries_key, task_metas_key },
        { task_id }
    )

    local meta_json = task_result.meta
    local pickups = task_result.pickups
    local retries = task_result.retries

    if task_result.state == "ok" then
        -- Check if cancelled
        if redis.call('SISMEMBER', task_cancellations_key, task_id) == 1 then
            -- Remove from cancelled set
            redis.call('SREM', task_cancellations_key, task_id)
            -- Add to agent cancelled queue
            redis.call('ZADD', queue_cancelled_key, timestamp, task_id)
            -- Update the task status to cancelled
            redis.call('HSET', task_statuses_key, task_id, 'cancelled')

            inc_metrics(
                { agent_metrics_bucket_key, global_metrics_bucket_key },
                { 'cancelled', pickups, retries, meta_json, metrics_ttl }
            )

            table.insert(tasks_to_cancel_ids, task_id)
        else
            -- Add to processing queue
            redis.call('ZADD', processing_heartbeats_key, timestamp, task_id)
            -- Update the task status to processing
            redis.call('HSET', task_statuses_key, task_id, 'processing')
            redis.call('HINCRBY', task_pickups_key, task_id, 1)

            table.insert(tasks_to_process_ids, task_id)
        end
    elseif task_result.state == "missing" then
        -- Add to orphaned tasks set with timestamp for investigation
        redis.call('ZADD', queue_orphaned_key, timestamp, task_id)
        table.insert(orphaned_task_ids, task_id)
    elseif task_result.state == "corrupted" then
        -- Task data is corrupted, skip it and log it
        table.insert(corrupted_task_ids, task_id)
    end
end

return { tasks_to_process_ids, tasks_to_cancel_ids, orphaned_task_ids, corrupted_task_ids }
