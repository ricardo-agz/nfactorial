---@diagnostic disable: undefined-global
--[[
-- Coordinate duplicate resolve_hook requests for the same hook callback.
--
-- External hook callbacks may be retried concurrently; this script provides an
-- atomic claim/finalize record so callers can distinguish fresh work, replay,
-- in-flight pending work, and request conflicts.
--
-- State transitions (record-level, not task status):
-- - key missing -> claimed (record state set to pending)
-- - pending + same request hash -> pending
-- - resolved + same request hash -> replay
-- - same key + different request hash -> conflict
-- - finalize -> record state set to resolved (with task_resumed outcome)
]]--
local hook_resolution_key = KEYS[1]

local mode = ARGV[1]
local request_hash = ARGV[2]
local ttl_seconds = tonumber(ARGV[3]) or 0
local hook_id = ARGV[4]
local task_id = ARGV[5]
local tool_call_id = ARGV[6]
local task_resumed_flag = ARGV[7]

if ttl_seconds < 1 then
    ttl_seconds = 1
end

local function parse_task_resumed(flag)
    if flag == "1" then
        return true
    elseif flag == "0" then
        return false
    end
    return nil
end

local function encode_task_resumed(value)
    if value == true then
        return "1"
    elseif value == false then
        return "0"
    end
    return ""
end

local function build_record(state, task_resumed)
    local record = {
        state = state,
        request_hash = request_hash,
        hook_id = hook_id,
        task_id = task_id,
        tool_call_id = tool_call_id,
    }
    if task_resumed ~= nil then
        record.task_resumed = task_resumed
    end
    return record
end

local function decode_record(raw)
    local ok, decoded = pcall(cjson.decode, raw)
    if not ok or type(decoded) ~= "table" then
        return nil
    end
    if type(decoded.request_hash) ~= "string" then
        return nil
    end
    if type(decoded.state) ~= "string" then
        return nil
    end
    return decoded
end

if mode == "claim" then
    local existing_raw = redis.call("GET", hook_resolution_key)
    if existing_raw then
        local existing = decode_record(existing_raw)
        if existing ~= nil then
            if existing.request_hash ~= request_hash then
                return { "conflict", encode_task_resumed(existing.task_resumed), existing.request_hash }
            end

            redis.call("EXPIRE", hook_resolution_key, ttl_seconds)
            if existing.state == "resolved" then
                return { "replay", encode_task_resumed(existing.task_resumed), existing.request_hash }
            end
            return { "pending", encode_task_resumed(existing.task_resumed), existing.request_hash }
        end
    end

    redis.call(
        "SET",
        hook_resolution_key,
        cjson.encode(build_record("pending", nil)),
        "EX",
        ttl_seconds
    )
    return { "claimed", "", request_hash }
end

if mode == "finalize" then
    local existing_raw = redis.call("GET", hook_resolution_key)
    if existing_raw then
        local existing = decode_record(existing_raw)
        if existing ~= nil and existing.request_hash ~= request_hash then
            return { "conflict", encode_task_resumed(existing.task_resumed), existing.request_hash }
        end
    end

    local task_resumed = parse_task_resumed(task_resumed_flag)
    redis.call(
        "SET",
        hook_resolution_key,
        cjson.encode(build_record("resolved", task_resumed)),
        "EX",
        ttl_seconds
    )
    return { "resolved", task_resumed_flag, request_hash }
end

return { "invalid_mode", "", request_hash }
