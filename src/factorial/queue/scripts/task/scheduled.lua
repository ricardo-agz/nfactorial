--[[
-- Wake paused tasks whose scheduled wait time has arrived.
--
-- Scheduled recovery atomically drains ready entries from the scheduled ZSET,
-- clears wait metadata, and requeues only tasks that are still paused.
--
-- State transitions:
-- - paused -> active (and LPUSH to main queue)
-- - missing task data -> orphaned queue marker
-- - any other status -> no transition (entry is consumed from scheduled queue)
]]--
local queue_scheduled_key = KEYS[1]
local queue_main_key = KEYS[2]
local queue_pending_key = KEYS[3]
local queue_orphaned_key = KEYS[4]
local task_statuses_key = KEYS[5]
local task_agents_key = KEYS[6]
local task_payloads_key = KEYS[7]
local task_pickups_key = KEYS[8]
local task_retries_key = KEYS[9]
local task_metas_key = KEYS[10]
local scheduled_wait_meta_key = KEYS[11]
local activity_wait_meta_key = KEYS[12]
local signal_wait_meta_key = KEYS[13]
local signal_wake_meta_key = KEYS[14]

local max_batch_size = tonumber(ARGV[1])

local time_result = redis.call('TIME')
local timestamp = tonumber(time_result[1]) + (tonumber(time_result[2]) / 1000000)

local ready_task_ids = redis.call('ZRANGEBYSCORE', queue_scheduled_key, 0, timestamp, 'LIMIT', 0, max_batch_size)
if #ready_task_ids == 0 then
    return {}
end

-- Remove from scheduled queue first to avoid duplicate recoveries.
for i = 1, #ready_task_ids, 1000 do
    local slice = {}
    local slice_end = math.min(i + 999, #ready_task_ids)
    for j = i, slice_end do
        table.insert(slice, ready_task_ids[j])
    end
    redis.call('ZREM', queue_scheduled_key, unpack(slice))
end

local recovered_task_ids = {}

for i = 1, #ready_task_ids do
    local task_id = ready_task_ids[i]
    local scheduled_wait_meta_raw = redis.call('HGET', scheduled_wait_meta_key, task_id)
    local task_result = load_task(
        { task_statuses_key, task_agents_key, task_payloads_key, task_pickups_key, task_retries_key, task_metas_key },
        { task_id }
    )

    -- This entry has been processed; clear wait metadata regardless of state.
    redis.call('HDEL', scheduled_wait_meta_key, task_id)

    if task_result.state == "ok" then
        if task_result.status == "paused" then
            -- Timeout fired while task was in activity wait: clear activity markers.
            if redis.call('HEXISTS', activity_wait_meta_key, task_id) == 1 then
                redis.call('HDEL', activity_wait_meta_key, task_id)
                redis.call('ZREM', queue_pending_key, task_id)
            end
            if signal_wake_meta_key and signal_wake_meta_key ~= "" then
                redis.call('HDEL', signal_wake_meta_key, task_id)
            end
            local signal_wait_meta_raw = nil
            if signal_wait_meta_key and signal_wait_meta_key ~= "" then
                signal_wait_meta_raw = redis.call('HGET', signal_wait_meta_key, task_id)
                if signal_wait_meta_raw and signal_wait_meta_raw ~= "" then
                    redis.call('HDEL', signal_wait_meta_key, task_id)
                    redis.call('ZREM', queue_pending_key, task_id)
                end
            end

            if signal_wait_meta_raw and signal_wait_meta_raw ~= ""
                and signal_wake_meta_key and signal_wake_meta_key ~= ""
                and scheduled_wait_meta_raw and scheduled_wait_meta_raw ~= ""
            then
                local wait_signal_id = nil
                local wait_ok, wait_meta = pcall(cjson.decode, signal_wait_meta_raw)
                if wait_ok and type(wait_meta) == "table" then
                    wait_signal_id = wait_meta.signal_id
                end

                local scheduled_ok, scheduled_meta = pcall(cjson.decode, scheduled_wait_meta_raw)
                local is_signal_timeout = false
                if scheduled_ok and type(scheduled_meta) == "table" then
                    is_signal_timeout = scheduled_meta.kind == "signal_timeout"
                end

                if is_signal_timeout and wait_signal_id then
                    redis.call(
                        'HSET',
                        signal_wake_meta_key,
                        task_id,
                        cjson.encode({
                            signal_id = wait_signal_id,
                            payload = cjson.null,
                            sender_task_id = cjson.null,
                            sent_at = timestamp,
                            seq = cjson.null,
                            wake_reason = "timeout",
                        })
                    )
                end
            end
            table.insert(recovered_task_ids, task_id)
        end
    elseif task_result.state == "missing" then
        redis.call('ZADD', queue_orphaned_key, timestamp, task_id)
    end
end

if #recovered_task_ids > 0 then
    for i = 1, #recovered_task_ids do
        redis.call('HSET', task_statuses_key, recovered_task_ids[i], "active")
    end
    redis.call('LPUSH', queue_main_key, unpack(recovered_task_ids))
end

return recovered_task_ids
