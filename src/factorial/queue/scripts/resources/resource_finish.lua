--[[
-- Finalize or abort a reserved resource lifecycle operation.
--
-- Modes:
-- - abort: revert an in-flight reservation back to a stable state
-- - commit_checkpoint: replace a live resource with a checkpoint or delete it
-- - commit_destroy: remove the binding after successful teardown
--]]--
local resource_bindings_key = KEYS[1]

local mode = ARGV[1]
local resource_field = ARGV[2]
local operation_id = ARGV[3]
local now_timestamp = tonumber(ARGV[4]) or 0
local checkpoint_json = ARGV[5]

local binding = resource_decode_binding(redis.call("HGET", resource_bindings_key, resource_field))
if not binding then
    return "missing"
end

if binding.operation_id ~= operation_id then
    return "operation_conflict"
end

if mode == "commit_destroy" then
    redis.call("HDEL", resource_bindings_key, resource_field)
    return "ok"
end

if mode == "commit_checkpoint" then
    if checkpoint_json and checkpoint_json ~= "" then
        binding.phase = RESOURCE_PHASE_CHECKPOINTED
        binding.live_ref = cjson.null
        binding.checkpoint = cjson.decode(checkpoint_json)
        binding.owner_pickups = cjson.null
        binding.operation_id = cjson.null
        binding.updated_at = now_timestamp
        redis.call("HSET", resource_bindings_key, resource_field, resource_encode_binding(binding))
        return "ok"
    end

    redis.call("HDEL", resource_bindings_key, resource_field)
    return "ok"
end

if mode == "abort" then
    local previous_phase = binding.phase
    binding.operation_id = cjson.null
    binding.owner_pickups = cjson.null
    binding.updated_at = now_timestamp
    if resource_binding_has_live_ref(binding)
        or previous_phase == RESOURCE_PHASE_CHECKPOINTING
        or previous_phase == RESOURCE_PHASE_DESTROYING
        or previous_phase == RESOURCE_PHASE_LIVE
    then
        binding.phase = RESOURCE_PHASE_LIVE
        redis.call("HSET", resource_bindings_key, resource_field, resource_encode_binding(binding))
        return "ok"
    end
    if resource_binding_has_checkpoint(binding) then
        binding.phase = RESOURCE_PHASE_CHECKPOINTED
        redis.call("HSET", resource_bindings_key, resource_field, resource_encode_binding(binding))
        return "ok"
    end
    redis.call("HDEL", resource_bindings_key, resource_field)
    return "ok"
end

return "invalid_mode"
