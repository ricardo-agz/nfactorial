--[[
-- Execute the post-turn task state machine for one task.
--
-- Turn completion requires coordinated updates across task hashes, queues,
-- pending wait structures, batch progress, parent-child linkage, and metrics.
-- This script keeps those transitions atomic.
--
-- State transitions (by action):
-- - complete: processing | pending_tool_results | pending_child_tasks -> completed
-- - continue: processing -> active
-- - retry: processing -> active (with retries incremented)
-- - backoff: processing -> backoff
-- - fail: processing | pending_tool_results | pending_child_tasks -> failed
-- - pending_tool_call_results: processing -> pending_tool_results
-- - pending_child_task_results: processing -> pending_child_tasks
]]--
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
local pending_child_wait_ids_key     = KEYS[22]
local parent_pending_child_tasks_key = KEYS[23]
local parent_pending_child_wait_ids_key = KEYS[24]

local task_id                        = ARGV[1]
local action                         = ARGV[2] -- complete, continue, retry, backoff, fail, pending_tool_call_results, pending_child_task_results
local updated_task_payload_json      = ARGV[3] -- Persisted for actions that mutate payload/state.
local current_turn                   = tonumber(ARGV[4])
local metrics_ttl                    = tonumber(ARGV[5])
local pending_sentinel               = ARGV[6]
-- Optional parameters - "" if not used
local pending_tool_call_ids_json     = ARGV[7]
local pending_child_task_ids_json    = ARGV[8]
local final_output_json              = ARGV[9]

-- Action constants
local ACTION_COMPLETE = "complete"
local ACTION_CONTINUE = "continue"
local ACTION_RETRY = "retry"
local ACTION_BACKOFF = "backoff"
local ACTION_FAIL = "fail"
local ACTION_PENDING_TOOL = "pending_tool_call_results"
local ACTION_PENDING_CHILD = "pending_child_task_results"

-- Task status constants
local STATUS_PROCESSING = "processing"
local STATUS_ACTIVE = "active"
local STATUS_PENDING_TOOL_RESULTS = "pending_tool_results"
local STATUS_PENDING_CHILD_TASKS = "pending_child_tasks"
local STATUS_COMPLETED = "completed"
local STATUS_BACKOFF = "backoff"
local STATUS_FAILED = "failed"

-- Metric labels
local METRIC_COMPLETED = "completed"
local METRIC_FAILED = "failed"
local METRIC_RETRIED = "retried"

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
local retries = task_result.retries
local meta = cjson.decode(meta_json)
local parent_task_id = meta.parent_id
local has_pending_tool_ids = pending_tool_call_ids_json ~= ""
local has_pending_child_ids = pending_child_task_ids_json ~= ""

local function should_write_parent_child_result(parent_id, child_id)
    if not parent_id then
        return false
    end
    if not parent_pending_child_tasks_key or parent_pending_child_tasks_key == "" then
        return false
    end

    local parent_status = redis.call('HGET', task_statuses_key, parent_id)
    if parent_status ~= STATUS_PENDING_CHILD_TASKS then
        return false
    end

    -- Parent wait-id set is required for child result writes.
    -- Without it, we cannot safely prove the parent still awaits this child.
    if not parent_pending_child_wait_ids_key or parent_pending_child_wait_ids_key == "" then
        return false
    end

    return redis.call('SISMEMBER', parent_pending_child_wait_ids_key, child_id) == 1
end

local function persist_parent_child_result_if_waiting(child_id, output_json)
    if should_write_parent_child_result(parent_task_id, child_id) then
        redis.call('HSET', parent_pending_child_tasks_key, child_id, output_json)
    end
end

local function transition_to_pending_tool_results()
    if status ~= STATUS_PROCESSING then
        -- Invalid: only processing tasks can transition into pending_tool_results.
        return { false, false }
    end
    if not has_pending_tool_ids then
        -- Invalid: pending_tool_call_results requires at least one tool-call id payload.
        return { false, false }
    end

    local pending_tool_call_ids = cjson.decode(pending_tool_call_ids_json)
    if next(pending_tool_call_ids) == nil then
        -- Invalid: empty tool id list would park the task with nothing to await.
        return { false, false }
    end

    for _, tool_call_id in ipairs(pending_tool_call_ids) do
        redis.call('HSET', pending_tool_results_key, tool_call_id, pending_sentinel)
    end

    redis.call('HSET', task_statuses_key, task_id, STATUS_PENDING_TOOL_RESULTS)
    redis.call('HSET', task_payloads_key, task_id, updated_task_payload_json)
    redis.call('ZADD', queue_pending_key, timestamp, task_id)

    return { true, false }
end

local function transition_to_pending_child_results()
    if status ~= STATUS_PROCESSING then
        -- Invalid: only processing tasks can transition into pending_child_tasks.
        return { false, false }
    end
    if not has_pending_child_ids then
        -- Invalid: pending_child_task_results requires child task id payload.
        return { false, false }
    end

    local pending_child_task_ids = cjson.decode(pending_child_task_ids_json)
    if next(pending_child_task_ids) == nil then
        -- Invalid: empty child id list would park the task with nothing to await.
        return { false, false }
    end

    -- Track exactly which child task IDs this pending wait is joining on.
    if pending_child_wait_ids_key and pending_child_wait_ids_key ~= "" then
        redis.call('DEL', pending_child_wait_ids_key)
    end
    for _, child_task_id in ipairs(pending_child_task_ids) do
        if pending_child_wait_ids_key and pending_child_wait_ids_key ~= "" then
            redis.call('SADD', pending_child_wait_ids_key, child_task_id)
        end
        -- Preserve already-completed child outputs if they landed before parent
        -- entered pending_child_tasks state.
        redis.call('HSETNX', pending_child_tasks_key, child_task_id, pending_sentinel)
    end

    redis.call('HSET', task_statuses_key, task_id, STATUS_PENDING_CHILD_TASKS)
    redis.call('HSET', task_payloads_key, task_id, updated_task_payload_json)
    redis.call('ZADD', queue_pending_key, timestamp, task_id)

    return { true, false }
end

-- Invariant hardening:
-- Pending transitions must be explicit action values.
-- Reject ambiguous calls where both pending payloads are provided.
if has_pending_tool_ids and has_pending_child_ids then
    -- Invalid: a single completion transition cannot wait on tools and children at once.
    return { false, false }
end

if action == ACTION_PENDING_TOOL then
    return transition_to_pending_tool_results()
end

if action == ACTION_PENDING_CHILD then
    return transition_to_pending_child_results()
end

-- For non-pending actions, callers must not pass pending payload lists.
if has_pending_tool_ids or has_pending_child_ids then
    -- Invalid: pending id payloads are only legal with explicit pending actions.
    return { false, false }
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

    if status == STATUS_COMPLETED or status == STATUS_FAILED then
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

if action == ACTION_COMPLETE then
    -- Check if task is in a valid state for completion
    if status ~= STATUS_PROCESSING and status ~= STATUS_PENDING_TOOL_RESULTS and status ~= STATUS_PENDING_CHILD_TASKS then
        return { false, false }
    end

    -- Task finished successfully, add to completions queue
    redis.call('ZADD', queue_completions_key, timestamp, task_id)
    -- Set task status to completed
    redis.call('HSET', task_statuses_key, task_id, STATUS_COMPLETED)
    -- Persist the latest payload so context changes (e.g., turn counter, messages) are not lost
    redis.call('HSET', task_payloads_key, task_id, updated_task_payload_json)

    -- Update timeline metrics
    inc_metrics(
        { agent_metrics_bucket_key, global_metrics_bucket_key },
        { METRIC_COMPLETED, meta_json, metrics_ttl }
    )

    -- If the task is a child task, update parent pending results only when the
    -- parent is still actively waiting on this child ID.
    persist_parent_child_result_if_waiting(task_id, final_output_json)

    local batch_completed = update_batch_progress(task_id, meta, current_turn, STATUS_COMPLETED)

    return { true, batch_completed }
elseif action == ACTION_CONTINUE then
    -- Check if task is in a valid state for continuation
    if status ~= STATUS_PROCESSING then
        return { false, false }
    end

    -- Task needs more processing, put back at front of main queue
    redis.call('LPUSH', queue_main_key, task_id)
    -- Update the task status and payload
    redis.call('HSET', task_statuses_key, task_id, STATUS_ACTIVE)
    redis.call('HSET', task_payloads_key, task_id, updated_task_payload_json)

    update_batch_progress(task_id, meta, current_turn, STATUS_ACTIVE)
    return { true, false }
elseif action == ACTION_RETRY then
    -- Check if task is in a valid state for retry
    if status ~= STATUS_PROCESSING then
        return { false, false }
    end

    -- Requeue and update the task status and retry count
    redis.call('LPUSH', queue_main_key, task_id)
    redis.call('HSET', task_statuses_key, task_id, STATUS_ACTIVE)
    redis.call('HINCRBY', task_retries_key, task_id, 1)
    redis.call('HSET', task_metas_key, task_id, cjson.encode(meta))

    -- Update timeline metrics for retry
    inc_metrics(
        { agent_metrics_bucket_key, global_metrics_bucket_key },
        { METRIC_RETRIED, meta_json, metrics_ttl }
    )

    return { true, false }
elseif action == ACTION_BACKOFF then
    -- Check if task is in a valid state for backoff
    if status ~= STATUS_PROCESSING then
        return { false, false }
    end

    -- Add to backoff queue with exponential backoff
    local backoff_duration = math.min(5 * 2 ^ retries, 300) -- Cap at 5 minutes
    redis.call('ZADD', queue_backoff_key, timestamp + backoff_duration, task_id)
    redis.call('HSET', task_statuses_key, task_id, STATUS_BACKOFF)
    redis.call('HINCRBY', task_retries_key, task_id, 1)
    redis.call('HSET', task_metas_key, task_id, cjson.encode(meta))

    return { true, false }
elseif action == ACTION_FAIL then
    -- Check if task is in a valid state for failure
    if status ~= STATUS_PROCESSING and status ~= STATUS_PENDING_TOOL_RESULTS and status ~= STATUS_PENDING_CHILD_TASKS then
        return { false, false }
    end

    -- Add to failed queue and set task status to failed
    redis.call('ZADD', queue_failed_key, timestamp, task_id)
    redis.call('HSET', task_statuses_key, task_id, STATUS_FAILED)

    -- Update timeline metrics
    inc_metrics(
        { agent_metrics_bucket_key, global_metrics_bucket_key },
        { METRIC_FAILED, meta_json, metrics_ttl }
    )

    -- If the task is a child task, update parent pending results only when the
    -- parent is still actively waiting on this child ID.
    persist_parent_child_result_if_waiting(task_id, final_output_json)

    local batch_completed = update_batch_progress(task_id, meta, current_turn, STATUS_FAILED)
    return { true, batch_completed }
else
    error("invalid_action")
end
