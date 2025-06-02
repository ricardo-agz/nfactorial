local agent_queue = KEYS[1]
local tasks_key = KEYS[2]
local task_id = ARGV[1]
local task_data_json = ARGV[2]

-- Push task_id to agent queue
redis.call('LPUSH', agent_queue, task_id)

-- Decode task data and set all fields in TASKS hash
local task_data = cjson.decode(task_data_json)
local hash_args = {}
for key, value in pairs(task_data) do
    table.insert(hash_args, key)
    -- Convert value to string if it's not already a string or number
    if type(value) == "table" then
        table.insert(hash_args, cjson.encode(value))
    else
        table.insert(hash_args, value)
    end
end

if #hash_args > 0 then
    redis.call('HSET', tasks_key, unpack(hash_args))
    return true
else
    error("Task data cannot be empty")
end 
