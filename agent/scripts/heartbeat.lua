local agent_heartbeats_key = KEYS[1]
local task_id = ARGV[1]

local time_result = redis.call('TIME')
local timestamp = tonumber(time_result[1]) + (tonumber(time_result[2]) / 1000000)

redis.call('ZADD', agent_heartbeats_key, timestamp, task_id)

return timestamp
