local queue_main_key                 = KEYS[1]
local queue_completions_key          = KEYS[2]
local queue_failed_key               = KEYS[3]
local queue_backoff_key              = KEYS[4]
local queue_orphaned_key             = KEYS[5]
local queue_pending_key              = KEYS[6]
local task_statuses_key              = KEYS[7]
local task_agents_key                = KEYS[8]
local task_payloads_key              = KEYS[9]
local task_pickups_key               = KEYS[10]
local task_retries_key               = KEYS[11]
local task_metas_key                 = KEYS[12]
local batch_meta_key                 = KEYS[13]
local batch_progress_key             = KEYS[14]
local batch_remaining_tasks_key      = KEYS[15]
local batch_completed_key            = KEYS[16]
local processing_heartbeats_key      = KEYS[17]
local pending_tool_results_key       = KEYS[18]
local pending_child_tasks_key        = KEYS[19]
local agent_metrics_bucket_key       = KEYS[20]
local global_metrics_bucket_key      = KEYS[21]
local parent_pending_child_tasks_key = KEYS[22]

local task_id                        = ARGV[1]
local action                         = ARGV[2] -- complete, continue, retry, fail, pending
local updated_task_payload_json      = ARGV[3] -- Only used for continue/fail
local current_turn                   = tonumber(ARGV[4])
local metrics_ttl                    = tonumber(ARGV[5])
local pending_sentinel               = ARGV[6]
-- Optional parameters - "" if not used
local pending_tool_call_ids_json     = ARGV[7]
local pending_child_task_ids_json    = ARGV[8]
local final_output_json              = ARGV[9]

local time_result                    = redis.call('TIME')
local timestamp                      = tonumber(time_result[1]) + (tonumber(time_result[2]) / 1000000)

-- Always remove from processing first
redis.call('ZREM', processing_heartbeats_key, task_id)

local task_result = load_task(
    { task_statuses_key, task_agents_key, task_payloads_key, task_pickups_key, task_retries_key, task_metas_key },
    { task_id }
)
if task_result.state == "missing" then
    -- Task is missing, add to orphaned queue
    redis.call('ZADD', queue_orphaned_key, timestamp, task_id)
    return { false, false }
elseif task_result.state == "corrupted" then
    -- Task data is corrupted, skip it and log it
    return { false, false }
end

local status = task_result.status
local meta_json = task_result.meta
local pickups = task_result.pickups
local retries = task_result.retries
local meta = cjson.decode(meta_json)
local parent_task_id = meta.parent_id

-- Handle pending tool call results only if both parameters are provided
if pending_tool_call_ids_json ~= "" then
    local pending_tool_call_ids = cjson.decode(pending_tool_call_ids_json)
    for _, tool_call_id in ipairs(pending_tool_call_ids) do
        redis.call('HSET', pending_tool_results_key, tool_call_id, pending_sentinel)
    end
    -- Set task status to pending tool call results
    redis.call('HSET', task_statuses_key, task_id, "pending_tool_results")
    -- Persist the latest payload so context changes (e.g., turn counter, messages) are not lost
    redis.call('HSET', task_payloads_key, task_id, updated_task_payload_json)
    -- Add to parked queue
    redis.call('ZADD', queue_pending_key, timestamp, task_id)

    return { true, false }
end

if pending_child_task_ids_json ~= "" then
    local pending_child_task_ids = cjson.decode(pending_child_task_ids_json)
    for _, child_task_id in ipairs(pending_child_task_ids) do
        redis.call('HSET', pending_child_tasks_key, child_task_id, pending_sentinel)
    end
    -- Set task status to pending child tasks
    redis.call('HSET', task_statuses_key, task_id, "pending_child_tasks")
    -- Persist the latest payload so context changes (e.g., turn counter, messages) are not lost
    redis.call('HSET', task_payloads_key, task_id, updated_task_payload_json)
    -- Add to parked queue
    redis.call('ZADD', queue_pending_key, timestamp, task_id)

    return { true, false }
end


local function update_batch_progress(task_id, task_meta, current_turn, status)
    -- Max progress is:
    -- if max_turns is set: num tasks * max turns per task
    -- if max_turns is not set: num tasks
    -- Therefore, each turn:
    -- - on continue, increase progress by 1 (if max_turns is set)
    -- - on complete/fail, increase progress by max_turns - current turn (if max_turns is set) else 1
    -- Then, the progress is just progress_sum / max_progress * 100

    -- Look up batch id for this task
    -- Guard against `cjson.null` which is returned when the JSON value is `null`.
    local batch_id = task_meta.batch_id
    if (not batch_id) or batch_id == cjson.null then
        return false
    end

    local max_turns = task_meta.max_turns
    if max_turns == cjson.null then
        max_turns = nil
    end

    local batch_meta_json = redis.call('HGET', batch_meta_key, batch_id)
    if not batch_meta_json then return false end
    local batch_meta = cjson.decode(batch_meta_json)
    local previous_status = batch_meta.status

    if status == "completed" or status == "failed" then
        if max_turns then
            redis.call('HINCRBY', batch_progress_key, batch_id, max_turns - current_turn)
        else
            redis.call('HINCRBY', batch_progress_key, batch_id, 1)
        end

        -- remove from remaining tasks
        local remaining_tasks = redis.call('HGET', batch_remaining_tasks_key, batch_id)
        if remaining_tasks then
            local remaining_tasks_list = cjson.decode(remaining_tasks)
            -- Find and remove the task_id from the list
            for i, id in ipairs(remaining_tasks_list) do
                if id == task_id then
                    table.remove(remaining_tasks_list, i)
                    break
                end
            end
            redis.call('HSET', batch_remaining_tasks_key, batch_id, cjson.encode(remaining_tasks_list))
        end

        -- if there are no remaining tasks, set batch status to completed and if the previous status was active, set to completed
        local remaining_tasks = redis.call('HGET', batch_remaining_tasks_key, batch_id)
        if remaining_tasks then
            local remaining_tasks_list = cjson.decode(remaining_tasks)
            if next(remaining_tasks_list) == nil then
                redis.call('ZADD', batch_completed_key, timestamp, batch_id)
                if previous_status == "active" then
                    batch_meta.status = "completed"
                    redis.call('HSET', batch_meta_key, batch_id, cjson.encode(batch_meta))
                    return true
                end
            end
        end
    else
        if max_turns then
            redis.call('HINCRBY', batch_progress_key, batch_id, 1)
        end
    end

    return false
end

if action == "complete" then
    -- Check if task is in a valid state for completion
    if status ~= "processing" and status ~= "pending_tool_results" and status ~= "pending_child_tasks" then
        return { false, false }
    end

    -- Task finished successfully, add to completions queue
    redis.call('ZADD', queue_completions_key, timestamp, task_id)
    -- Set task status to completed
    redis.call('HSET', task_statuses_key, task_id, "completed")
    -- Persist the latest payload so context changes (e.g., turn counter, messages) are not lost
    redis.call('HSET', task_payloads_key, task_id, updated_task_payload_json)

    -- Update timeline metrics
    inc_metrics(
        { agent_metrics_bucket_key, global_metrics_bucket_key },
        { 'completed', pickups, retries, meta_json, metrics_ttl }
    )

    -- If the task is a child task, update the parent task's pending child task results
    if parent_task_id and parent_pending_child_tasks_key then
        redis.call('HSET', parent_pending_child_tasks_key, task_id, final_output_json)
    end

    local batch_completed = update_batch_progress(task_id, meta, current_turn, "completed")

    return { true, batch_completed }
elseif action == "continue" then
    -- Check if task is in a valid state for continuation
    if status ~= "processing" then
        return { false, false }
    end

    -- Task needs more processing, put back at front of main queue
    redis.call('LPUSH', queue_main_key, task_id)
    -- Update the task status and payload
    redis.call('HSET', task_statuses_key, task_id, "active")
    redis.call('HSET', task_payloads_key, task_id, updated_task_payload_json)

    update_batch_progress(task_id, meta, current_turn, "active")
    return { true, false }
elseif action == "retry" then
    -- Check if task is in a valid state for retry
    if status ~= "processing" then
        return { false, false }
    end

    -- Requeue and update the task status and retry count
    redis.call('LPUSH', queue_main_key, task_id)
    redis.call('HSET', task_statuses_key, task_id, "active")
    redis.call('HINCRBY', task_retries_key, task_id, 1)
    redis.call('HSET', task_metas_key, task_id, cjson.encode(meta))

    -- Update timeline metrics for retry
    inc_metrics(
        { agent_metrics_bucket_key, global_metrics_bucket_key },
        { 'retried', pickups, retries, meta_json, metrics_ttl }
    )

    return { true, false }
elseif action == "backoff" then
    -- Check if task is in a valid state for backoff
    if status ~= "processing" then
        return { false, false }
    end

    -- Add to backoff queue with exponential backoff
    local backoff_duration = math.min(5 * 2 ^ retries, 300) -- Cap at 5 minutes
    redis.call('ZADD', queue_backoff_key, timestamp + backoff_duration, task_id)
    redis.call('HSET', task_statuses_key, task_id, "backoff")
    redis.call('HINCRBY', task_retries_key, task_id, 1)
    redis.call('HSET', task_metas_key, task_id, cjson.encode(meta))

    return { true, false }
elseif action == "fail" then
    -- Check if task is in a valid state for failure
    if status ~= "processing" and status ~= "pending_tool_results" and status ~= "pending_child_tasks" then
        return { false, false }
    end

    -- Add to failed queue and set task status to failed
    redis.call('ZADD', queue_failed_key, timestamp, task_id)
    redis.call('HSET', task_statuses_key, task_id, "failed")

    -- Update timeline metrics
    inc_metrics(
        { agent_metrics_bucket_key, global_metrics_bucket_key },
        { 'failed', pickups, retries, meta_json, metrics_ttl }
    )

    -- If the task is a child task, update the parent task's pending child task results
    if parent_task_id and parent_pending_child_tasks_key then
        redis.call('HSET', parent_pending_child_tasks_key, task_id, final_output_json)
    end

    local batch_completed = update_batch_progress(task_id, meta, current_turn, "failed")
    return { true, batch_completed }
else
    error("invalid_action")
end
