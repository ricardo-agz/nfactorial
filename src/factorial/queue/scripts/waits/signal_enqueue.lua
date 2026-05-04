--[[
-- Deliver a signal to a task and wake it when waiting for the same signal id.
--
-- Signals are stored per-task by signal_id (latest writer wins) and are consumed
-- automatically by wait.until_signal when matched.
--
-- State transitions:
-- - paused(signal wait, matching signal_id) -> active (and LPUSH to main queue)
-- - non-terminal task -> signal persisted for future wait consumption
-- - terminal task -> no wake, treated as inactive target
-- - missing/corrupted task data -> failure
]]--
local task_statuses_key = KEYS[1]
local task_agents_key = KEYS[2]
local task_payloads_key = KEYS[3]
local task_pickups_key = KEYS[4]
local task_retries_key = KEYS[5]
local task_metas_key = KEYS[6]
local signal_wait_meta_key = KEYS[7]
local signal_wake_meta_key = KEYS[8]
local task_signals_key = KEYS[9]
local queue_main_key_template = KEYS[10]
local queue_pending_key_template = KEYS[11]
local queue_scheduled_key_template = KEYS[12]
local scheduled_wait_meta_key = KEYS[13]
local signal_seq_key = KEYS[14]

local sender_task_id = ARGV[1]
local task_id = ARGV[2]
local signal_id = ARGV[3]
local payload_json = ARGV[4]

local function _format_template_key(template, token, value)
    return string.gsub(template, token, value)
end

local time_result = redis.call('TIME')
local timestamp = tonumber(time_result[1]) + (tonumber(time_result[2]) / 1000000)

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
    return { false, "missing", 0, 0 }
elseif task_result.state == "corrupted" then
    return { false, "corrupted", 0, 0 }
end

if task_result.status == "completed"
    or task_result.status == "failed"
    or task_result.status == "cancelled"
then
    return { true, "inactive", 0, 0 }
end

local payload = cjson.null
if payload_json and payload_json ~= "" then
    local ok, decoded = pcall(cjson.decode, payload_json)
    if ok then
        payload = decoded
    end
end

local signal_seq = redis.call('INCR', signal_seq_key)
local envelope = cjson.encode({
    signal_id = signal_id,
    payload = payload,
    sender_task_id = sender_task_id,
    sent_at = timestamp,
    seq = signal_seq,
})

redis.call('HSET', task_signals_key, signal_id, envelope)

local woken = false
if task_result.status == "paused" and redis.call('HEXISTS', signal_wait_meta_key, task_id) == 1 then
    local wait_meta_raw = redis.call('HGET', signal_wait_meta_key, task_id)
    local expected_signal_id = nil
    if wait_meta_raw and wait_meta_raw ~= "" then
        local ok, wait_meta = pcall(cjson.decode, wait_meta_raw)
        if ok and type(wait_meta) == "table" then
            expected_signal_id = wait_meta.signal_id
        end
    end

    if expected_signal_id == signal_id then
        local queue_main_key = _format_template_key(
            queue_main_key_template,
            "{agent}",
            task_result.agent
        )
        local queue_pending_key = _format_template_key(
            queue_pending_key_template,
            "{agent}",
            task_result.agent
        )
        local queue_scheduled_key = _format_template_key(
            queue_scheduled_key_template,
            "{agent}",
            task_result.agent
        )

        redis.call('HSET', task_statuses_key, task_id, "active")
        redis.call('HDEL', signal_wait_meta_key, task_id)
        redis.call('HSET', signal_wake_meta_key, task_id, envelope)
        redis.call('HDEL', task_signals_key, signal_id)
        redis.call('ZREM', queue_pending_key, task_id)
        redis.call('ZREM', queue_scheduled_key, task_id)
        redis.call('HDEL', scheduled_wait_meta_key, task_id)
        redis.call('LPUSH', queue_main_key, task_id)
        woken = true
    end
end

return { true, "sent", woken and 1 or 0, signal_seq }
