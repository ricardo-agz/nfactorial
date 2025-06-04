local pending_tool_results_key = KEYS[1]
local main_queue = KEYS[2]
local tasks_key = KEYS[3]
local task_id = ARGV[1]
local updated_task_context_json = ARGV[2]

-- Check if the pending tool results key exists
if redis.call('EXISTS', pending_tool_results_key) == 0 then
    return false
end

-- Clear the pending tool results, update task data, and enqueue task ID
redis.call('DEL', pending_tool_results_key)
redis.call('HSET', tasks_key, 'payload', updated_task_context_json)
redis.call('HSET', tasks_key, 'status', 'active')
redis.call('LPUSH', main_queue, task_id)

return true 