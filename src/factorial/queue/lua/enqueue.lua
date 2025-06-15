local agent_queue_key = KEYS[1]
local task_statuses_key = KEYS[2]
local task_agents_key = KEYS[3]
local task_payloads_key = KEYS[4]
local task_pickups_key = KEYS[5]
local task_retries_key = KEYS[6]
local task_metas_key = KEYS[7]

local task_id = ARGV[1]
local task_agent = ARGV[2]
local task_payload_json = ARGV[3]
local task_pickups = tonumber(ARGV[4])
local task_retries = tonumber(ARGV[5])
local task_meta_json = ARGV[6]

-- Push task_id to the back of the agent queue
redis.call('RPUSH', agent_queue_key, task_id)

-- Set task data
redis.call('HSET', task_statuses_key, task_id, "queued")
redis.call('HSET', task_agents_key, task_id, task_agent)
redis.call('HSET', task_payloads_key, task_id, task_payload_json)
redis.call('HSET', task_pickups_key, task_id, task_pickups)
redis.call('HSET', task_retries_key, task_id, task_retries)
redis.call('HSET', task_metas_key, task_id, task_meta_json)

return true
