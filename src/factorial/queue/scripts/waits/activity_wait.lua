--[[
-- Park a task until external activity is observed.
--
-- Activity waits atomically remove heartbeat tracking, persist wait metadata,
-- and move eligible tasks into paused/activity state. After parking, the
-- script can immediately wake the direct parent when the parent's subtree is
-- fully quiescent (all children terminal or paused(activity)).
--
-- State transitions:
-- - processing | active -> paused (activity wait)
-- - missing task data -> orphaned queue marker
-- - invalid status -> no transition
]]--
local queue_pending_key = KEYS[1]
local queue_orphaned_key = KEYS[2]
local processing_heartbeats_key = KEYS[3]
local task_statuses_key = KEYS[4]
local task_agents_key = KEYS[5]
local task_payloads_key = KEYS[6]
local task_pickups_key = KEYS[7]
local task_retries_key = KEYS[8]
local task_metas_key = KEYS[9]
local activity_wait_meta_key = KEYS[10]
local message_seq_key = KEYS[11]
local queue_scheduled_key_template = KEYS[12]
local scheduled_wait_meta_key = KEYS[13]

local task_id = ARGV[1]
local updated_task_payload_json = ARGV[2]
local wait_metadata_json = ARGV[3]
local task_steering_key_template = ARGV[4]
local task_children_key_template = ARGV[5]
local queue_main_key_template = ARGV[6]
local queue_pending_key_template = ARGV[7]
local timeout_wake_timestamp = tonumber(ARGV[8])
local scheduled_wait_metadata_json = ARGV[9]

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
    redis.call('HDEL', activity_wait_meta_key, task_id)
    return { false, "missing", 0 }
elseif task_result.state == "corrupted" then
    return { false, "corrupted", 0 }
end

if task_result.status ~= "processing" and task_result.status ~= "active" then
    return { false, "invalid_status", 0 }
end

redis.call('HSET', task_statuses_key, task_id, "paused")
redis.call('HSET', task_payloads_key, task_id, updated_task_payload_json)
redis.call('HSET', activity_wait_meta_key, task_id, wait_metadata_json)
redis.call('ZADD', queue_pending_key, timestamp, task_id)
if timeout_wake_timestamp then
    if queue_scheduled_key_template and queue_scheduled_key_template ~= "" then
        local queue_scheduled_key = _format_template_key(
            queue_scheduled_key_template,
            "{agent}",
            task_result.agent
        )
        redis.call('ZADD', queue_scheduled_key, timeout_wake_timestamp, task_id)
    end
    if scheduled_wait_meta_key and scheduled_wait_meta_key ~= ""
        and scheduled_wait_metadata_json and scheduled_wait_metadata_json ~= ""
    then
        redis.call('HSET', scheduled_wait_meta_key, task_id, scheduled_wait_metadata_json)
    end
end

local task_meta = cjson.decode(task_result.meta)
local parent_task_id = task_meta.parent_id

-- Self-probe first: if this task's direct subtree is already quiescent, wake it
-- immediately to avoid ordering deadlocks (children parked before parent).
local self_woken = maybe_wake_parent_on_subtree_idle(
    {
        task_statuses_key,
        task_agents_key,
        queue_main_key_template,
        queue_pending_key_template,
        activity_wait_meta_key,
        task_steering_key_template,
        message_seq_key,
        task_children_key_template,
        queue_scheduled_key_template,
        scheduled_wait_meta_key,
    },
    {
        task_id,
        task_id,
    }
)
if self_woken then
    return { true, "ok", 1 }
end

local parent_woken = maybe_wake_parent_on_subtree_idle(
    {
        task_statuses_key,
        task_agents_key,
        queue_main_key_template,
        queue_pending_key_template,
        activity_wait_meta_key,
        task_steering_key_template,
        message_seq_key,
        task_children_key_template,
        queue_scheduled_key_template,
        scheduled_wait_meta_key,
    },
    {
        parent_task_id,
        task_id,
    }
)

return { true, "ok", parent_woken and 1 or 0 }
