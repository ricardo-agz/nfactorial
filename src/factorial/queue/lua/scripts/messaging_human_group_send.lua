--[[
-- Send a human-originated message to all members of a team-scoped group atomically.
--
-- The operation validates group scope, persists history, and fans out steering
-- envelopes to active members in one transaction.
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

local team_id = ARGV[1]
local group_name = ARGV[2]
local content = ARGV[3]
local metadata_json = ARGV[4]
local steering_key_template = ARGV[5]
local history_maxlen = tonumber(ARGV[6]) or 0
local queue_main_key_template = ARGV[7]
local queue_pending_key_template = ARGV[8]
local queue_scheduled_key_template = ARGV[9]
local groups_by_task_key_template = ARGV[10]
local from_owner_id = ARGV[11]
local from_task_id = ARGV[12]

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

if not from_owner_id or from_owner_id == "" then
    return { "invalid_owner" }
end

if redis.call("HEXISTS", group_meta_key, group_name) == 0 then
    return { "group_not_found" }
end

local metadata = {}
if metadata_json and metadata_json ~= "" then
    local ok, decoded = pcall(cjson.decode, metadata_json)
    if ok and type(decoded) == "table" then
        metadata = decoded
    end
end

local time_result = redis.call("TIME")
local timestamp_s = tonumber(time_result[1]) + (tonumber(time_result[2]) / 1000000)
local timestamp_ms = math.floor(timestamp_s * 1000)

local delivered_task_ids = {}
local skipped_inactive_task_ids = {}
local failed_task_ids = {}
local to_task_ids = {}

local from_task_attr = ""
if from_task_id and from_task_id ~= "" then
    from_task_attr = " from_task_id='" .. tostring(from_task_id) .. "'"
end
local steering_content = "<peer_message kind='human_group' team_id='" ..
    tostring(team_id) ..
    "' group='" ..
    tostring(group_name) ..
    "' from_owner_id='" ..
    tostring(from_owner_id) ..
    "'" ..
    from_task_attr ..
    ">" ..
    tostring(content) ..
    "</peer_message>"
local steering_payload = cjson.encode({
    role = "user",
    content = steering_content,
    metadata = metadata,
})

local members = redis.call("SMEMBERS", group_members_key)
for _, member_task_id in ipairs(members) do
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

if history_maxlen < 1 then
    history_maxlen = 20000
end
local from_task_value = cjson.null
if from_task_id and from_task_id ~= "" then
    from_task_value = from_task_id
end
local history_payload = cjson.encode({
    kind = "human_group",
    team_id = team_id,
    group_name = group_name,
    from_task_id = from_task_value,
    from_owner_id = from_owner_id,
    to_task_ids = to_task_ids,
    delivered_task_ids = delivered_task_ids,
    skipped_inactive_task_ids = skipped_inactive_task_ids,
    failed_task_ids = failed_task_ids,
    content = content,
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
