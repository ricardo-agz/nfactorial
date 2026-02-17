--[[
-- Pop tasks from the main queue and stage them for worker processing.
--
-- Worker pickup atomically handles cancellation races, heartbeat placement,
-- pickup counters, and orphan/corruption detection while filling a batch.
--
-- State transitions:
-- - queued | active -> processing (heartbeat set, pickups incremented)
-- - task listed in pending_cancellations -> cancelled (moved to cancelled queue)
-- - missing task data -> orphaned queue marker
-- - corrupted task data -> skipped and reported as corrupted
]]--
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
local activity_wait_meta_key = KEYS[14]
local queue_pending_key_template = KEYS[15]
local queue_main_key_template = KEYS[16]
local task_steering_key_template = KEYS[17]
local message_seq_key = KEYS[18]

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
    -- Stop once we have enough valid tasks
    if #tasks_to_process_ids >= batch_size then
        break
    end

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
                { 'cancelled', meta_json, metrics_ttl }
            )

            if activity_wait_meta_key and activity_wait_meta_key ~= ""
                and queue_pending_key_template and queue_pending_key_template ~= ""
                and queue_main_key_template and queue_main_key_template ~= ""
                and task_steering_key_template and task_steering_key_template ~= ""
                and message_seq_key and message_seq_key ~= ""
            then
                local meta = cjson.decode(meta_json)
                local parent_task_id = meta.parent_id
                if parent_task_id and parent_task_id ~= cjson.null then
                    wake_parent_on_child_terminal(
                        {
                            task_statuses_key,
                            task_agents_key,
                            queue_main_key_template,
                            queue_pending_key_template,
                            activity_wait_meta_key,
                            task_steering_key_template,
                            message_seq_key,
                        },
                        {
                            parent_task_id,
                            task_id,
                        }
                    )
                end
            end

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
