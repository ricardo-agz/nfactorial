local queue_orphaned_key = KEYS[1]
local task_statuses_key = KEYS[2]
local task_agents_key = KEYS[3]
local task_payloads_key = KEYS[4]
local task_pickups_key = KEYS[5]
local task_retries_key = KEYS[6]
local task_metas_key = KEYS[7]
local steering_messages_key = KEYS[8]

local task_id = ARGV[1]
local steering_message_ids = ARGV[2]
local updated_task_payload_json = ARGV[3]

local time_result = redis.call('TIME')
local timestamp = tonumber(time_result[1]) + (tonumber(time_result[2]) / 1000000)

local task_result = load_task(
    { task_statuses_key, task_agents_key, task_payloads_key, task_pickups_key, task_retries_key, task_metas_key },
    { task_id }
)

-- Always delete steering messages
for _, id in ipairs(cjson.decode(steering_message_ids)) do
    redis.call('HDEL', steering_messages_key, id)
end

if task_result.state == "ok" then
    -- Update task with steering message
    redis.call('HSET', task_payloads_key, task_id, updated_task_payload_json)
    return { true, "ok" }
elseif task_result.state == "missing" then
    -- Task is missing, add to orphaned queue
    redis.call('ZADD', queue_orphaned_key, timestamp, task_id)
    return { false, "missing" }
elseif task_result.state == "corrupted" then
    -- Task data is corrupted, just clean up and fail
    return { false, "corrupted" }
end
