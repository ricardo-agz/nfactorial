--[[
-- Remove members from an existing team-scoped messaging group atomically.
--
-- Validates sender membership and candidate member scope, then updates group
-- membership and per-task reverse indexes in one transaction.
--
-- State transitions:
-- - group exists -> members removed (idempotent)
-- - group missing -> no-op (group_not_found)
]]--
local task_metas_key = KEYS[1]
local group_meta_key = KEYS[2]
local group_members_key = KEYS[3]

local sender_task_id = ARGV[1]
local team_id = ARGV[2]
local group_name = ARGV[3]
local member_task_ids_json = ARGV[4]
local groups_by_task_key_template = ARGV[5]

local function groups_by_task_key(task_id)
    return string.gsub(groups_by_task_key_template, "{task_id}", task_id)
end

local function decode_meta(raw_meta)
    if not raw_meta then
        return nil
    end
    local ok, decoded = pcall(cjson.decode, raw_meta)
    if not ok or type(decoded) ~= "table" then
        return nil
    end
    return decoded
end

local function resolve_team_id(task_id, meta)
    if not meta then
        return nil
    end
    local meta_team_id = meta.team_id
    if type(meta_team_id) == "string" and meta_team_id ~= "" then
        return meta_team_id
    end
    return task_id
end

if redis.call("HEXISTS", group_meta_key, group_name) == 0 then
    return { "group_not_found" }
end

local sender_meta_raw = redis.call("HGET", task_metas_key, sender_task_id)
local sender_meta = decode_meta(sender_meta_raw)
if sender_meta == nil then
    return { "sender_not_found" }
end
local sender_team_id = resolve_team_id(sender_task_id, sender_meta)
if sender_team_id ~= team_id then
    return { "scope_mismatch", sender_team_id or "" }
end
if redis.call("SISMEMBER", group_members_key, sender_task_id) == 0 then
    return { "sender_not_member" }
end

local member_task_ids = cjson.decode(member_task_ids_json)
local unique_member_task_ids = {}
local seen = {}

for _, member_task_id in ipairs(member_task_ids) do
    if type(member_task_id) == "string" and member_task_id ~= "" and not seen[member_task_id] then
        seen[member_task_id] = true
        table.insert(unique_member_task_ids, member_task_id)
    end
end

for _, member_task_id in ipairs(unique_member_task_ids) do
    local member_meta_raw = redis.call("HGET", task_metas_key, member_task_id)
    local member_meta = decode_meta(member_meta_raw)
    if member_meta == nil then
        return { "member_not_found", member_task_id }
    end
    local member_team_id = resolve_team_id(member_task_id, member_meta)
    if member_team_id ~= team_id then
        return { "member_scope_mismatch", member_task_id, member_team_id or "" }
    end
end

local removed_member_task_ids = {}
for _, member_task_id in ipairs(unique_member_task_ids) do
    local removed = redis.call("SREM", group_members_key, member_task_id)
    redis.call("SREM", groups_by_task_key(member_task_id), group_name)
    if removed == 1 then
        table.insert(removed_member_task_ids, member_task_id)
    end
end

return { "updated", cjson.encode(removed_member_task_ids) }
