--[[
-- Send a message to all members of a team-scoped group atomically.
--
-- The operation validates sender scope and membership, persists history, and
-- fans out steering envelopes to active members in one transaction.
--
-- State transitions:
-- - no task status transition; task payload updates happen later in steering
-- - history streams append one canonical message record
]]--
local task_statuses_key = KEYS[1]
local task_agents_key = KEYS[2]
local task_metas_key = KEYS[3]
local group_meta_key = KEYS[4]
local group_members_key = KEYS[5]
local thread_history_key = KEYS[6]
local global_history_key = KEYS[7]
local message_seq_key = KEYS[8]
local activity_wait_meta_key = KEYS[9]
local scheduled_wait_meta_key = KEYS[10]
local team_tasks_key = KEYS[11]

local sender_task_id = ARGV[1]
local team_id = ARGV[2]
local group_name = ARGV[3]
local content = ARGV[4]
local data_json = ARGV[5]
local metadata_json = ARGV[6]
local steering_key_template = ARGV[7]
local history_maxlen = tonumber(ARGV[8]) or 0
local queue_main_key_template = ARGV[9]
local queue_pending_key_template = ARGV[10]
local queue_scheduled_key_template = ARGV[11]
local groups_by_task_key_template = ARGV[12]

local function steering_key(task_id)
    return string.gsub(steering_key_template, "{task_id}", task_id)
end

local function queue_main_key(agent_name)
    return string.gsub(queue_main_key_template, "{agent}", agent_name)
end

local function queue_pending_key(agent_name)
    return string.gsub(queue_pending_key_template, "{agent}", agent_name)
end

local function queue_scheduled_key(agent_name)
    return string.gsub(queue_scheduled_key_template, "{agent}", agent_name)
end

local function groups_by_task_key(task_id)
    if not groups_by_task_key_template or groups_by_task_key_template == "" then
        return ""
    end
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

local function is_terminal(status)
    return status == "completed" or status == "failed" or status == "cancelled"
end

local function prune_stale_member(task_id)
    redis.call("SREM", group_members_key, task_id)
    if team_tasks_key and team_tasks_key ~= "" then
        redis.call("SREM", team_tasks_key, task_id)
    end
    local member_groups_key = groups_by_task_key(task_id)
    if member_groups_key ~= "" then
        redis.call("SREM", member_groups_key, group_name)
    end
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

local metadata = {}
if metadata_json and metadata_json ~= "" then
    local ok, decoded = pcall(cjson.decode, metadata_json)
    if ok and type(decoded) == "table" then
        metadata = decoded
    end
end

local data = cjson.null
if data_json and data_json ~= "" then
    local ok, decoded = pcall(cjson.decode, data_json)
    if ok then
        data = decoded
    end
end

local time_result = redis.call("TIME")
local timestamp_s = tonumber(time_result[1]) + (tonumber(time_result[2]) / 1000000)
local timestamp_ms = math.floor(timestamp_s * 1000)

local delivered_task_ids = {}
local skipped_inactive_task_ids = {}
local failed_task_ids = {}
local to_task_ids = {}

local steering_content = "<peer_message kind='group' team_id='" ..
    tostring(team_id) ..
    "' group='" ..
    tostring(group_name) ..
    "' from_task_id='" ..
    tostring(sender_task_id) ..
    "'>" ..
    tostring(content) ..
    "</peer_message>"
local steering_payload = cjson.encode({
    role = "user",
    content = steering_content,
    data = data,
    metadata = metadata,
})

local members = redis.call("SMEMBERS", group_members_key)
for _, member_task_id in ipairs(members) do
    if member_task_id ~= sender_task_id then
        table.insert(to_task_ids, member_task_id)
        local member_status = redis.call("HGET", task_statuses_key, member_task_id)
        if not member_status then
            table.insert(failed_task_ids, member_task_id)
            prune_stale_member(member_task_id)
        elseif is_terminal(member_status) then
            table.insert(skipped_inactive_task_ids, member_task_id)
        else
            local seq = redis.call("INCR", message_seq_key)
            local message_id = tostring(timestamp_ms) .. "_" .. tostring(seq)
            redis.call(
                "HSET",
                steering_key(member_task_id),
                message_id,
                steering_payload
            )
            if member_status == "paused" and redis.call("HEXISTS", activity_wait_meta_key, member_task_id) == 1 then
                local member_agent = redis.call("HGET", task_agents_key, member_task_id)
                if member_agent then
                    redis.call("HSET", task_statuses_key, member_task_id, "active")
                    redis.call("HDEL", activity_wait_meta_key, member_task_id)
                    if scheduled_wait_meta_key and scheduled_wait_meta_key ~= "" then
                        redis.call("HDEL", scheduled_wait_meta_key, member_task_id)
                    end
                    redis.call("ZREM", queue_pending_key(member_agent), member_task_id)
                    if queue_scheduled_key_template and queue_scheduled_key_template ~= "" then
                        redis.call("ZREM", queue_scheduled_key(member_agent), member_task_id)
                    end
                    redis.call("LPUSH", queue_main_key(member_agent), member_task_id)
                end
            end
            table.insert(delivered_task_ids, member_task_id)
        end
    end
end

if history_maxlen < 1 then
    history_maxlen = 20000
end
local history_payload = cjson.encode({
    kind = "group",
    team_id = team_id,
    group_name = group_name,
    from_task_id = sender_task_id,
    to_task_ids = to_task_ids,
    delivered_task_ids = delivered_task_ids,
    skipped_inactive_task_ids = skipped_inactive_task_ids,
    failed_task_ids = failed_task_ids,
    content = content,
    data = data,
    metadata = metadata,
    created_at = timestamp_s,
})

local thread_message_id = redis.call(
    "XADD",
    thread_history_key,
    "MAXLEN",
    "~",
    history_maxlen,
    "*",
    "payload",
    history_payload
)

local global_message_id = redis.call(
    "XADD",
    global_history_key,
    "MAXLEN",
    "~",
    history_maxlen,
    "*",
    "payload",
    history_payload
)

return {
    "sent",
    tostring(thread_message_id),
    tostring(global_message_id),
    cjson.encode(delivered_task_ids),
    cjson.encode(skipped_inactive_task_ids),
    cjson.encode(failed_task_ids),
}
