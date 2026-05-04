--[[
-- Reserve a task-scoped resource binding for an external lifecycle operation.
--
-- Resource lifecycle work spans Redis state and external side effects, so we use
-- an atomic reservation step before doing any provider call. This script fences
-- stale workers using the task's current processing pickup count and converts
-- abandoned in-flight reservations back into stable states after a timeout.
--
-- Modes:
-- - acquire: reserve create / restore / attach-live
-- - checkpoint: reserve checkpointing of an existing live resource
-- - destroy: reserve terminal destroy while the current worker still owns the task
-- - system_destroy: reserve destroy from a non-processing control-plane path
--]]--
local task_statuses_key = KEYS[1]
local task_pickups_key = KEYS[2]
local resource_bindings_key = KEYS[3]

local mode = ARGV[1]
local task_id = ARGV[2]
local resource_field = ARGV[3]
local binding_metadata_json = ARGV[4]
local expected_pickups = tonumber(ARGV[5])
local operation_id = ARGV[6]
local now_timestamp = tonumber(ARGV[7]) or 0
local operation_timeout_s = tonumber(ARGV[8]) or 0

local function current_task_state()
    local status = redis.call("HGET", task_statuses_key, task_id)
    local pickups = tonumber(redis.call("HGET", task_pickups_key, task_id))
    return status, pickups
end

if mode == "acquire" or mode == "checkpoint" or mode == "destroy" then
    local current_status, current_pickups = current_task_state()
    if current_status ~= "processing" or current_pickups ~= expected_pickups then
        return { "stale_owner", "" }
    end
elseif mode == "system_destroy" then
    local current_status = current_task_state()
    if current_status == "processing" then
        return { "not_allowed", "" }
    end
else
    return { "invalid_mode", "" }
end

local raw_binding = redis.call("HGET", resource_bindings_key, resource_field)
local binding = resource_decode_binding(raw_binding)
local recovered_binding, mutated, busy = resource_recover_binding(
    binding,
    now_timestamp,
    operation_timeout_s
)
binding = recovered_binding

if mutated then
    if binding == nil then
        redis.call("HDEL", resource_bindings_key, resource_field)
    else
        redis.call("HSET", resource_bindings_key, resource_field, resource_encode_binding(binding))
    end
end

if busy then
    return { "busy", binding and resource_encode_binding(binding) or "" }
end

local binding_json = binding and resource_encode_binding(binding) or ""
local binding_metadata = {}
if binding_metadata_json and binding_metadata_json ~= "" then
    local ok, decoded_metadata = pcall(cjson.decode, binding_metadata_json)
    if ok and type(decoded_metadata) == "table" then
        binding_metadata = decoded_metadata
    end
end
local binding_metadata_for_create = binding_metadata
if binding ~= nil and type(binding.binding_metadata) == "table" then
    if next(binding.binding_metadata) ~= nil or next(binding_metadata) == nil then
        binding_metadata_for_create = binding.binding_metadata
    end
end

if mode == "acquire" then
    if binding == nil then
        local creating = {
            resource_type_key = string.match(resource_field, "^(.*):[^:]+$") or resource_field,
            logical_name = string.match(resource_field, ".*:([^:]+)$") or "default",
            binding_metadata = binding_metadata_for_create,
            phase = RESOURCE_PHASE_CREATING,
            owner_pickups = expected_pickups,
            operation_id = operation_id,
            updated_at = now_timestamp,
        }
        redis.call("HSET", resource_bindings_key, resource_field, resource_encode_binding(creating))
        return { "create", "" }
    end

    if resource_binding_has_live_ref(binding) then
        binding.phase = RESOURCE_PHASE_ATTACHING
        binding.owner_pickups = expected_pickups
        binding.operation_id = operation_id
        binding.updated_at = now_timestamp
        redis.call("HSET", resource_bindings_key, resource_field, resource_encode_binding(binding))
        return { "attach", binding_json }
    end

    if resource_binding_has_checkpoint(binding) then
        binding.phase = RESOURCE_PHASE_RESTORING
        binding.owner_pickups = expected_pickups
        binding.operation_id = operation_id
        binding.updated_at = now_timestamp
        redis.call("HSET", resource_bindings_key, resource_field, resource_encode_binding(binding))
        return { "restore", binding_json }
    end

    local creating = {
        resource_type_key = binding.resource_type_key,
        logical_name = binding.logical_name,
        binding_metadata = binding_metadata_for_create,
        phase = RESOURCE_PHASE_CREATING,
        owner_pickups = expected_pickups,
        operation_id = operation_id,
        updated_at = now_timestamp,
    }
    redis.call("HSET", resource_bindings_key, resource_field, resource_encode_binding(creating))
    return { "create", binding_json }
end

if binding == nil then
    return { "missing", "" }
end

if mode == "checkpoint" then
    if binding.phase ~= RESOURCE_PHASE_LIVE and not resource_binding_has_live_ref(binding) then
        return { "missing", binding_json }
    end
    binding.phase = RESOURCE_PHASE_CHECKPOINTING
    binding.owner_pickups = expected_pickups
    binding.operation_id = operation_id
    binding.updated_at = now_timestamp
    redis.call("HSET", resource_bindings_key, resource_field, resource_encode_binding(binding))
    return { "ok", binding_json }
end

if mode == "destroy" or mode == "system_destroy" then
    binding.phase = RESOURCE_PHASE_DESTROYING
    binding.owner_pickups = mode == "destroy" and expected_pickups or cjson.null
    binding.operation_id = operation_id
    binding.updated_at = now_timestamp
    redis.call("HSET", resource_bindings_key, resource_field, resource_encode_binding(binding))
    return { "ok", binding_json }
end

return { "invalid_mode", binding_json }
