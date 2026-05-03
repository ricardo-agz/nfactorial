--[[
-- Enqueue steering messages and wake tasks parked in activity waits.
--
-- The operation atomically validates target task liveness, appends steering
-- messages, and requeues paused(activity) tasks so activity waits are resumed
-- immediately when new steering arrives.
--
-- State transitions:
-- - paused(activity) -> active (and LPUSH to main queue)
-- - missing task data -> orphaned queue marker
-- - terminal tasks -> no transition (inactive)
]]--
local queue_main_key = KEYS[1]
local queue_orphaned_key = KEYS[2]
local queue_pending_key = KEYS[3]
local queue_scheduled_key = KEYS[4]
local task_statuses_key = KEYS[5]
local task_agents_key = KEYS[6]
local task_payloads_key = KEYS[7]
local task_pickups_key = KEYS[8]
local task_retries_key = KEYS[9]
local task_metas_key = KEYS[10]
local steering_messages_key = KEYS[11]
local activity_wait_meta_key = KEYS[12]
local scheduled_wait_meta_key = KEYS[13]
local message_seq_key = KEYS[14]

local task_id = ARGV[1]
local messages_json = ARGV[2]

local time_result = redis.call('TIME')
local timestamp = tonumber(time_result[1]) + (tonumber(time_result[2]) / 1000000)
local timestamp_ms = math.floor(timestamp * 1000)

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
    return { false, "missing", 0 }
elseif task_result.state == "corrupted" then
    return { false, "corrupted", 0 }
end

if task_result.status == "completed" or task_result.status == "failed" or task_result.status == "cancelled" then
    return { false, "inactive", 0 }
end

local messages = cjson.decode(messages_json)
for _, message_entry in ipairs(messages) do
    local message_payload_json = ""
    if type(message_entry) == "string" then
        message_payload_json = message_entry
    else
        message_payload_json = cjson.encode(message_entry)
    end
    local seq = redis.call('INCR', message_seq_key)
    local message_id = tostring(timestamp_ms) .. "_" .. tostring(seq)
    redis.call('HSET', steering_messages_key, message_id, message_payload_json)
end

local woke = false
if task_result.status == "paused" and redis.call("HEXISTS", activity_wait_meta_key, task_id) == 1 then
    redis.call("HSET", task_statuses_key, task_id, "active")
    redis.call("HDEL", activity_wait_meta_key, task_id)
    redis.call("HDEL", scheduled_wait_meta_key, task_id)
    redis.call("ZREM", queue_pending_key, task_id)
    redis.call("ZREM", queue_scheduled_key, task_id)
    redis.call("LPUSH", queue_main_key, task_id)
    woke = true
end

return { true, "ok", woke and 1 or 0 }
