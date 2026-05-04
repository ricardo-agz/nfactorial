--[[
-- Park a task until an explicit signal is delivered.
--
-- Signal waits are cooperative: the task opts in by returning wait.until_signal.
-- If a matching signal is already pending for this task, the script skips parking
-- and immediately re-queues the task as active.
--
-- State transitions:
-- - processing | active -> paused (signal wait)
-- - processing | active -> active (already_signaled fast path)
-- - missing task data -> orphaned queue marker
-- - invalid status -> no transition
]]--
local queue_main_key = KEYS[1]
local queue_pending_key = KEYS[2]
local queue_orphaned_key = KEYS[3]
local processing_heartbeats_key = KEYS[4]
local task_statuses_key = KEYS[5]
local task_agents_key = KEYS[6]
local task_payloads_key = KEYS[7]
local task_pickups_key = KEYS[8]
local task_retries_key = KEYS[9]
local task_metas_key = KEYS[10]
local signal_wait_meta_key = KEYS[11]
local signal_wake_meta_key = KEYS[12]
local task_signals_key = KEYS[13]
local queue_scheduled_key = KEYS[14]
local scheduled_wait_meta_key = KEYS[15]

local task_id = ARGV[1]
local signal_id = ARGV[2]
local updated_task_payload_json = ARGV[3]
local wait_metadata_json = ARGV[4]
local timeout_wake_timestamp = tonumber(ARGV[5])
local scheduled_wait_metadata_json = ARGV[6]

local time_result = redis.call('TIME')
local timestamp = tonumber(time_result[1]) + (tonumber(time_result[2]) / 1000000)

redis.call('ZREM', processing_heartbeats_key, task_id)

local task_result = load_task(
    {
        task_statuses_key,
        task_agents_key,
        task_payloads_key,
        task_pickups_key,
        task_retries_key,
        task_metas_key,
    },
    { task_id }
)

if task_result.state == "missing" then
    redis.call('ZADD', queue_orphaned_key, timestamp, task_id)
    redis.call('HDEL', signal_wait_meta_key, task_id)
    redis.call('HDEL', signal_wake_meta_key, task_id)
    return { false, "missing", 0 }
elseif task_result.state == "corrupted" then
    return { false, "corrupted", 0 }
end

if task_result.status ~= "processing" and task_result.status ~= "active" then
    return { false, "invalid_status", 0 }
end

local pending_signal = redis.call('HGET', task_signals_key, signal_id)
if pending_signal and pending_signal ~= "" then
    redis.call('HDEL', task_signals_key, signal_id)
    redis.call('HSET', signal_wake_meta_key, task_id, pending_signal)
    redis.call('HSET', task_statuses_key, task_id, "active")
    redis.call('HSET', task_payloads_key, task_id, updated_task_payload_json)
    redis.call('HDEL', signal_wait_meta_key, task_id)
    redis.call('ZREM', queue_pending_key, task_id)
    if queue_scheduled_key and queue_scheduled_key ~= "" then
        redis.call('ZREM', queue_scheduled_key, task_id)
    end
    if scheduled_wait_meta_key and scheduled_wait_meta_key ~= "" then
        redis.call('HDEL', scheduled_wait_meta_key, task_id)
    end
    redis.call('LPUSH', queue_main_key, task_id)
    return { true, "already_signaled", 1 }
end

redis.call('HSET', task_statuses_key, task_id, "paused")
redis.call('HSET', task_payloads_key, task_id, updated_task_payload_json)
redis.call('HSET', signal_wait_meta_key, task_id, wait_metadata_json)
redis.call('HDEL', signal_wake_meta_key, task_id)
redis.call('ZADD', queue_pending_key, timestamp, task_id)

if timeout_wake_timestamp then
    if queue_scheduled_key and queue_scheduled_key ~= "" then
        redis.call('ZADD', queue_scheduled_key, timeout_wake_timestamp, task_id)
    end
    if scheduled_wait_meta_key and scheduled_wait_meta_key ~= ""
        and scheduled_wait_metadata_json and scheduled_wait_metadata_json ~= ""
    then
        redis.call('HSET', scheduled_wait_meta_key, task_id, scheduled_wait_metadata_json)
    end
else
    if queue_scheduled_key and queue_scheduled_key ~= "" then
        redis.call('ZREM', queue_scheduled_key, task_id)
    end
    if scheduled_wait_meta_key and scheduled_wait_meta_key ~= "" then
        redis.call('HDEL', scheduled_wait_meta_key, task_id)
    end
end

return { true, "ok", 0 }
