local queue_scheduled_key = KEYS[1]
local queue_pending_key = KEYS[2]
local queue_orphaned_key = KEYS[3]
local processing_heartbeats_key = KEYS[4]
local task_statuses_key = KEYS[5]
local task_agents_key = KEYS[6]
local task_payloads_key = KEYS[7]
local task_pickups_key = KEYS[8]
local task_retries_key = KEYS[9]
local task_metas_key = KEYS[10]
local scheduled_wait_meta_key = KEYS[11]

local task_id = ARGV[1]
local updated_task_payload_json = ARGV[2]
local wake_timestamp = tonumber(ARGV[3])
local wait_metadata_json = ARGV[4]

local time_result = redis.call('TIME')
local timestamp = tonumber(time_result[1]) + (tonumber(time_result[2]) / 1000000)

-- Always remove from processing first.
redis.call('ZREM', processing_heartbeats_key, task_id)

local task_result = load_task(
    { task_statuses_key, task_agents_key, task_payloads_key, task_pickups_key, task_retries_key, task_metas_key },
    { task_id }
)

if task_result.state == "missing" then
    redis.call('ZADD', queue_orphaned_key, timestamp, task_id)
    redis.call('HDEL', scheduled_wait_meta_key, task_id)
    return { false, "missing" }
elseif task_result.state == "corrupted" then
    return { false, "corrupted" }
end

if not wake_timestamp then
    return { false, "invalid_wake_timestamp" }
end

local status = task_result.status
if status ~= "processing" and status ~= "active" then
    return { false, "invalid_status" }
end

redis.call('HSET', task_statuses_key, task_id, "paused")
redis.call('HSET', task_payloads_key, task_id, updated_task_payload_json)
redis.call('HSET', scheduled_wait_meta_key, task_id, wait_metadata_json)
redis.call('ZADD', queue_scheduled_key, wake_timestamp, task_id)
redis.call('ZREM', queue_pending_key, task_id)

return { true, "ok" }
