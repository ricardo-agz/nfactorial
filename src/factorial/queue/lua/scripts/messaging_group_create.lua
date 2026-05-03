--[[
-- Create a team-scoped messaging group atomically.
--
-- Group creation validates sender scope, validates all initial members, writes
-- metadata, materializes membership, and updates per-task reverse indexes in
-- one transaction.
--
-- State transitions:
-- - group missing -> created
-- - group exists  -> no-op (exists)
]]--
local task_metas_key = KEYS[1]
local group_meta_key = KEYS[2]
local group_members_key = KEYS[3]
local team_tasks_key = KEYS[4]

local sender_task_id = ARGV[1]
local team_id = ARGV[2]
local group_name = ARGV[3]
local group_meta_json = ARGV[4]
local member_task_ids_json = ARGV[5]
local groups_by_task_key_template = ARGV[6]

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

if group_name == nil or group_name == "" then
    return { "invalid_group_name" }
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

if redis.call("HEXISTS", group_meta_key, group_name) == 1 then
    return { "exists" }
end

local member_task_ids = cjson.decode(member_task_ids_json)
local unique_member_task_ids = {}
local seen = {}

local function add_member(task_id)
    if type(task_id) ~= "string" or task_id == "" then
        return
    end
    if seen[task_id] then
        return
    end
    seen[task_id] = true
    table.insert(unique_member_task_ids, task_id)
end

add_member(sender_task_id)
for _, member_task_id in ipairs(member_task_ids) do
    add_member(member_task_id)
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

redis.call("HSET", group_meta_key, group_name, group_meta_json)
for _, member_task_id in ipairs(unique_member_task_ids) do
    redis.call("SADD", group_members_key, member_task_id)
    redis.call("SADD", groups_by_task_key(member_task_id), group_name)
    redis.call("SADD", team_tasks_key, member_task_id)
end

return { "created", cjson.encode(unique_member_task_ids) }
