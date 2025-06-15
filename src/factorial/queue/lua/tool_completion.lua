local queue_main_key = KEYS[1]
local queue_orphaned_key = KEYS[2]
local queue_pending_key = KEYS[3]
local pending_tool_results_key = KEYS[4]
local task_statuses_key = KEYS[5]
local task_agents_key = KEYS[6]
local task_payloads_key = KEYS[7]
local task_pickups_key = KEYS[8]
local task_retries_key = KEYS[9]
local task_metas_key = KEYS[10]

local task_id = ARGV[1]
local updated_task_context_json = ARGV[2]

local time_result = redis.call('TIME')
local timestamp = tonumber(time_result[1]) + (tonumber(time_result[2]) / 1000000)

local task_result = load_task(
    { task_statuses_key, task_agents_key, task_payloads_key, task_pickups_key, task_retries_key, task_metas_key },
    { task_id }
)

-- Always delete the pending tool results and remove from parked queue
redis.call('DEL', pending_tool_results_key)
redis.call('ZREM', queue_pending_key, task_id)

if task_result.state == "ok" then
    -- Only move the task back to the main queue if it is still waiting for
    -- tool call results (status == 'pending_tool_results'). This prevents a
    -- race condition where multiple concurrent completions for the same task
    -- could re-enqueue the task more than once.
    if task_result.status ~= 'pending_tool_results' then
        return { false, 'already_completed' }
    end

    redis.call('HSET', task_payloads_key, task_id, updated_task_context_json)
    redis.call('HSET', task_statuses_key, task_id, 'active')
    redis.call('LPUSH', queue_main_key, task_id)
    return { true, 'ok' }
elseif task_result.state == "missing" then
    -- Task is missing, add to the orphaned queue
    redis.call('ZADD', queue_orphaned_key, timestamp, task_id)
    return { false, 'missing' }
elseif task_result.state == "corrupted" then
    return { false, 'corrupted' }
end

return { true, 'ok' }
