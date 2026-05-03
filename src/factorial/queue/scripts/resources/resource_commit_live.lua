--[[
-- Finalize a live resource acquisition after the provider operation succeeds.
--
-- This script fences stale workers using the task's current processing pickup
-- count and only commits the live ref when the reservation's operation_id still
-- matches the stored binding.
--]]--
local task_statuses_key = KEYS[1]
local task_pickups_key = KEYS[2]
local resource_bindings_key = KEYS[3]

local task_id = ARGV[1]
local resource_field = ARGV[2]
local expected_pickups = tonumber(ARGV[3])
local operation_id = ARGV[4]
local now_timestamp = tonumber(ARGV[5]) or 0
local live_ref_json = ARGV[6]
local checkpoint_json = ARGV[7]

local current_status = redis.call("HGET", task_statuses_key, task_id)
local current_pickups = tonumber(redis.call("HGET", task_pickups_key, task_id))
if current_status ~= "processing" or current_pickups ~= expected_pickups then
    return "stale_owner"
end

local binding = resource_decode_binding(redis.call("HGET", resource_bindings_key, resource_field))
if not binding then
    return "missing"
end

if binding.operation_id ~= operation_id then
    return "operation_conflict"
end

binding.phase = RESOURCE_PHASE_LIVE
binding.owner_pickups = expected_pickups
binding.operation_id = cjson.null
binding.updated_at = now_timestamp
if live_ref_json and live_ref_json ~= "" then
    binding.live_ref = cjson.decode(live_ref_json)
else
    binding.live_ref = cjson.null
end
if checkpoint_json and checkpoint_json ~= "" then
    binding.checkpoint = cjson.decode(checkpoint_json)
end

redis.call("HSET", resource_bindings_key, resource_field, resource_encode_binding(binding))
return "ok"
