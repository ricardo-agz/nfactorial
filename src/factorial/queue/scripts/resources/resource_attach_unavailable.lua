--[[
-- Commit the result of an attach-live reservation when the provider proves that
-- the stored live reference is no longer usable.
--
-- This is intentionally a commit, not an abort: aborting would restore the same
-- live reference and the next acquire would retry attach forever. The operation
-- is fenced by both the task pickup lease and the reservation operation_id.
--]]--
local task_statuses_key = KEYS[1]
local task_pickups_key = KEYS[2]
local resource_bindings_key = KEYS[3]

local task_id = ARGV[1]
local resource_field = ARGV[2]
local expected_pickups = tonumber(ARGV[3])
local operation_id = ARGV[4]
local now_timestamp = tonumber(ARGV[5]) or 0

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

binding.live_ref = cjson.null
binding.owner_pickups = cjson.null
binding.operation_id = cjson.null
binding.updated_at = now_timestamp

if resource_binding_has_checkpoint(binding) then
    binding.phase = RESOURCE_PHASE_CHECKPOINTED
else
    binding.phase = RESOURCE_PHASE_FRESH
end

redis.call("HSET", resource_bindings_key, resource_field, resource_encode_binding(binding))
return "ok"
