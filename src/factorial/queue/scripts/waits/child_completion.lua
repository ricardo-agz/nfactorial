--[[
-- Resume a parent task after child-task results are applied.
--
-- Parent tasks parked in pending_child_tasks need atomic cleanup of pending
-- child markers plus a safe requeue back to the main queue.
--
-- State transitions:
-- - pending_child_tasks -> active (and LPUSH to main queue)
-- - missing task data -> orphaned queue marker
-- - any other status -> no transition (returns already_completed)
]]--
local queue_main_key = KEYS[1]
local queue_orphaned_key = KEYS[2]
local queue_pending_key = KEYS[3]
local pending_child_task_results_key = KEYS[4]
local pending_child_wait_ids_key = KEYS[5]
local task_statuses_key = KEYS[6]
local task_agents_key = KEYS[7]
local task_payloads_key = KEYS[8]
local task_pickups_key = KEYS[9]
local task_retries_key = KEYS[10]
local task_metas_key = KEYS[11]
local activity_wait_meta_key = KEYS[12]

local task_id = ARGV[1]
local updated_task_context_json = ARGV[2]
local expected_wait_child_ids_json = ARGV[3]
local expected_result_values_json = ARGV[4]
local expected_child_statuses_json = ARGV[5]
local expected_child_activity_waiting_json = ARGV[6]

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
    redis.call('ZADD', queue_orphaned_key, timestamp, task_id)
    return { false, 'missing' }
elseif task_result.state == "corrupted" then
    return { false, 'corrupted' }
end

if task_result.state == "ok" and task_result.status ~= 'pending_child_tasks' then
    return { false, 'already_completed' }
end

if not pending_child_wait_ids_key or pending_child_wait_ids_key == "" then
    return { false, 'missing_wait_set' }
end

if expected_wait_child_ids_json == "" or expected_result_values_json == ""
    or expected_child_statuses_json == ""
    or expected_child_activity_waiting_json == ""
then
    return { false, 'missing_compare_state' }
end

local expected_wait_child_ids = cjson.decode(expected_wait_child_ids_json)
local expected_result_values = cjson.decode(expected_result_values_json)
local expected_child_statuses = cjson.decode(expected_child_statuses_json)
local expected_child_activity_waiting = cjson.decode(expected_child_activity_waiting_json)

if #expected_wait_child_ids == 0 then
    return { false, 'missing_wait_set' }
end

if #expected_wait_child_ids ~= #expected_result_values
    or #expected_wait_child_ids ~= #expected_child_statuses
    or #expected_wait_child_ids ~= #expected_child_activity_waiting
then
    return { false, 'invalid_compare_state' }
end

if tonumber(redis.call('SCARD', pending_child_wait_ids_key)) ~= #expected_wait_child_ids then
    return { false, 'stale_wait_set' }
end

local function expected_value(value)
    if value == cjson.null then
        return nil
    end
    return value
end

for idx, child_id in ipairs(expected_wait_child_ids) do
    if redis.call('SISMEMBER', pending_child_wait_ids_key, child_id) ~= 1 then
        return { false, 'stale_wait_set' }
    end

    local expected_result = expected_value(expected_result_values[idx])
    local actual_result = redis.call('HGET', pending_child_task_results_key, child_id)
    if actual_result == false then
        actual_result = nil
    end
    if actual_result ~= expected_result then
        return { false, 'stale_child_result' }
    end

    local expected_status = expected_value(expected_child_statuses[idx])
    local actual_status = redis.call('HGET', task_statuses_key, child_id)
    if actual_status == false then
        actual_status = nil
    end
    if actual_status ~= expected_status then
        return { false, 'stale_child_status' }
    end

    local actual_activity_waiting = false
    if activity_wait_meta_key and activity_wait_meta_key ~= "" then
        actual_activity_waiting = redis.call('HGET', activity_wait_meta_key, child_id) ~= false
    end
    if actual_activity_waiting ~= expected_child_activity_waiting[idx] then
        return { false, 'stale_child_activity' }
    end
end

local wait_child_ids = {}
if pending_child_wait_ids_key and pending_child_wait_ids_key ~= "" then
    wait_child_ids = redis.call('SMEMBERS', pending_child_wait_ids_key)
end
if next(wait_child_ids) ~= nil then
    redis.call('HDEL', pending_child_task_results_key, unpack(wait_child_ids))
else
    redis.call('DEL', pending_child_task_results_key)
end
if pending_child_wait_ids_key and pending_child_wait_ids_key ~= "" then
    redis.call('DEL', pending_child_wait_ids_key)
end
redis.call('ZREM', queue_pending_key, task_id)

redis.call('HSET', task_payloads_key, task_id, updated_task_context_json)
redis.call('HSET', task_statuses_key, task_id, 'active')
redis.call('LPUSH', queue_main_key, task_id)
return { true, 'ok' }
