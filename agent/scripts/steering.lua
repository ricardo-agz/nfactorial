local task_data_key = KEYS[1]
local steering_messages_key = KEYS[2]
local steering_message_ids = ARGV[1]
local updated_task_payload_json = ARGV[2]

-- Update task with steering message
redis.call('HSET', task_data_key, "payload", updated_task_payload_json)
-- Delete each steering message
for _, id in ipairs(cjson.decode(steering_message_ids)) do
    redis.call('HDEL', steering_messages_key, id)
end
