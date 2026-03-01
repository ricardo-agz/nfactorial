--[[
-- Shared helper functions used by all queue Lua scripts.
--
-- These helpers centralize canonical task loading semantics and bounded metrics
-- updates so behavior stays consistent across scripts.
--
-- State model returned by load_task():
-- - missing: no task hash fields found
-- - corrupted: partial task hash fields found
-- - ok: all required task hash fields found
--
-- This file does not perform task-state transitions directly.
]]--
--[[
---------------------------------------------------------------------
Usage:
local task_result = load_task(
    { task_statuses_key, task_agents_key, task_payloads_key, task_pickups_key, task_retries_key, task_metas_key },
    { task_id }
)
Returns:
{
    state = "ok", -- or "missing" or "corrupted"
    status = status,
    agent = agent,
    payload = payload,
    pickups = pickups,
    retries = retries,
    meta = meta,
}
---------------------------------------------------------------------
]] --
local function load_task(keys, args)
    local task_statuses_key = keys[1]
    local task_agents_key   = keys[2]
    local task_payloads_key = keys[3]
    local task_pickups_key  = keys[4]
    local task_retries_key  = keys[5]
    local task_metas_key    = keys[6]
    local task_id           = args[1]

    local status            = redis.call('HGET', task_statuses_key, task_id)
    local agent             = redis.call('HGET', task_agents_key, task_id)
    local payload           = redis.call('HGET', task_payloads_key, task_id)
    local pickups           = redis.call('HGET', task_pickups_key, task_id)
    local retries           = redis.call('HGET', task_retries_key, task_id)
    local meta              = redis.call('HGET', task_metas_key, task_id)

    if not (status or agent or payload or pickups or retries or meta) then
        return {
            state = "missing",
        }
    end
    if not (status and agent and payload and pickups and retries and meta) then
        return {
            state = "corrupted",
            status = status,
            agent = agent,
            payload = payload,
            pickups = tonumber(pickups or -1),
            retries = tonumber(retries or -1),
            meta = meta,
        }
    end
    return {
        state = "ok",
        status = status,
        agent = agent,
        payload = payload,
        pickups = tonumber(pickups),
        retries = tonumber(retries),
        meta = meta,
    }
end

--[[
---------------------------------------------------------------------
Usage:
inc_metrics(
    { agent_metrics_bucket_key, global_metrics_bucket_key },
    { 'cancelled', meta_json, metrics_ttl }
)

Returns:
true
---------------------------------------------------------------------
]] --
local function inc_metrics(keys, args)
    local agent_metrics_bucket_key  = keys[1]
    local global_metrics_bucket_key = keys[2]
    local metric_type               = args[1]
    local task_meta_json            = args[2]
    local ttl                       = args[3]

    local time_result               = redis.call('TIME')
    local timestamp                 = tonumber(time_result[1]) + (tonumber(time_result[2]) / 1000000)

    local meta                      = cjson.decode(task_meta_json)
    local duration                  = timestamp - (tonumber(meta.created_at) or timestamp)

    -- We only track a small, fixed set of metrics used by the dashboard.
    -- This keeps memory bounded and avoids unbounded key growth.
    local SUPPORTED = {
        completed = true,
        failed = true,
        cancelled = true,
        retried = true,
    }

    if not SUPPORTED[metric_type] then
        return true
    end

    -- Rolling ring buffers:
    -- - m1: 1-minute buckets, 24h retention (1440 slots)
    -- - m6: 6-minute buckets, 7d retention (1680 slots)
    --
    -- Field layout (HASH):
    --   {prefix}:ts:{slot}                       -> bucket_start_ts (seconds)
    --   {prefix}:{metric}_count:{slot}          -> count
    --   {prefix}:completed_total_duration:{slot} -> sum(duration_seconds)
    local function update_ring(key, prefix, bucket_seconds, ring_size)
        local bucket_ts = math.floor(timestamp / bucket_seconds) * bucket_seconds
        local bucket_id = math.floor(bucket_ts / bucket_seconds)
        local slot = bucket_id % ring_size

        local ts_field = prefix .. ':ts:' .. slot
        local existing_ts = redis.call('HGET', key, ts_field)

        if existing_ts ~= tostring(bucket_ts) then
            -- Reset slot for the new time bucket
            local args = { ts_field, bucket_ts }
            args[#args + 1] = prefix .. ':completed_count:' .. slot
            args[#args + 1] = 0
            args[#args + 1] = prefix .. ':failed_count:' .. slot
            args[#args + 1] = 0
            args[#args + 1] = prefix .. ':cancelled_count:' .. slot
            args[#args + 1] = 0
            args[#args + 1] = prefix .. ':retried_count:' .. slot
            args[#args + 1] = 0
            args[#args + 1] = prefix .. ':completed_total_duration:' .. slot
            args[#args + 1] = 0
            redis.call('HSET', key, unpack(args))
        end

        redis.call('HINCRBY', key, prefix .. ':' .. metric_type .. '_count:' .. slot, 1)
        if metric_type == 'completed' then
            redis.call('HINCRBYFLOAT', key, prefix .. ':completed_total_duration:' .. slot, duration)
        end
    end

    -- Update both agent + global keys for both rings
    update_ring(agent_metrics_bucket_key, 'm1', 60, 1440)
    update_ring(global_metrics_bucket_key, 'm1', 60, 1440)
    update_ring(agent_metrics_bucket_key, 'm6', 360, 1680)
    update_ring(global_metrics_bucket_key, 'm6', 360, 1680)

    -- Expire the whole hash eventually if the system goes idle.
    -- Ensure TTL is >= ~8d so the 7d ring doesn't disappear between bursts.
    local min_ttl = 8 * 24 * 60 * 60
    local ttl_seconds = tonumber(ttl) or 0
    if ttl_seconds < min_ttl then
        ttl_seconds = min_ttl
    end
    redis.call('EXPIRE', agent_metrics_bucket_key, ttl_seconds)
    redis.call('EXPIRE', global_metrics_bucket_key, ttl_seconds)

    return true
end


local function _format_template_key(template, token, value)
    return string.gsub(template, token, value)
end


local function _append_steering_message(task_steering_key, message_content, message_seq_key)
    local time_result = redis.call('TIME')
    local timestamp_ms = math.floor(
        tonumber(time_result[1]) * 1000 + (tonumber(time_result[2]) / 1000)
    )
    local seq = redis.call('INCR', message_seq_key)
    local message_id = tostring(timestamp_ms) .. "_" .. tostring(seq)
    redis.call(
        'HSET',
        task_steering_key,
        message_id,
        cjson.encode({ role = "user", content = message_content })
    )
    return message_id
end


--[[
---------------------------------------------------------------------
Wake a paused(activity) task and optionally enqueue a synthetic steering message.

Usage:
wake_task_if_waiting_activity(
    {
        task_statuses_key,
        task_agents_key,
        queue_main_key_template,
        queue_pending_key_template,
        activity_wait_meta_key,
        task_steering_key_template,
        message_seq_key,
        queue_scheduled_key_template, -- optional
        scheduled_wait_meta_key,      -- optional
    },
    {
        task_id,
        message_content, -- optional, empty string to skip synthetic steering append
    }
)

Returns:
true when the task was woken, false otherwise.
---------------------------------------------------------------------
]] --
local function wake_task_if_waiting_activity(keys, args)
    local task_statuses_key = keys[1]
    local task_agents_key = keys[2]
    local queue_main_key_template = keys[3]
    local queue_pending_key_template = keys[4]
    local activity_wait_meta_key = keys[5]
    local task_steering_key_template = keys[6]
    local message_seq_key = keys[7]
    local queue_scheduled_key_template = keys[8]
    local scheduled_wait_meta_key = keys[9]

    local task_id = args[1]
    local message_content = args[2] or ""

    local task_status = redis.call('HGET', task_statuses_key, task_id)
    if task_status ~= "paused" then
        return false
    end

    if redis.call('HEXISTS', activity_wait_meta_key, task_id) ~= 1 then
        return false
    end

    local task_agent = redis.call('HGET', task_agents_key, task_id)
    if not task_agent then
        return false
    end
    local queue_main_key = _format_template_key(
        queue_main_key_template,
        "{agent}",
        task_agent
    )
    local queue_pending_key = _format_template_key(
        queue_pending_key_template,
        "{agent}",
        task_agent
    )
    local queue_scheduled_key = nil
    if queue_scheduled_key_template and queue_scheduled_key_template ~= "" then
        queue_scheduled_key = _format_template_key(
            queue_scheduled_key_template,
            "{agent}",
            task_agent
        )
    end

    if message_content ~= "" then
        local task_steering_key = _format_template_key(
            task_steering_key_template,
            "{task_id}",
            task_id
        )
        _append_steering_message(task_steering_key, message_content, message_seq_key)
    end

    redis.call('HSET', task_statuses_key, task_id, "active")
    redis.call('HDEL', activity_wait_meta_key, task_id)
    redis.call('ZREM', queue_pending_key, task_id)
    if queue_scheduled_key then
        redis.call('ZREM', queue_scheduled_key, task_id)
    end
    if scheduled_wait_meta_key and scheduled_wait_meta_key ~= "" then
        redis.call('HDEL', scheduled_wait_meta_key, task_id)
    end
    redis.call('LPUSH', queue_main_key, task_id)

    return true
end


--[[
---------------------------------------------------------------------
Wake a parent waiting on activity when its direct subtree is quiescent.

Direct children are considered quiescent when they are terminal or paused(activity).
Paused(sleep/cron) counts as busy (self-waking, not deadlocked).

Usage:
maybe_wake_parent_on_subtree_idle(
    {
        task_statuses_key,
        task_agents_key,
        queue_main_key_template,
        queue_pending_key_template,
        activity_wait_meta_key,
        task_steering_key_template,
        message_seq_key,
        task_children_key_template,
        queue_scheduled_key_template, -- optional
        scheduled_wait_meta_key,      -- optional
    },
    {
        parent_task_id,
        source_task_id,
    }
)

Returns:
true when parent was woken, false otherwise.
---------------------------------------------------------------------
]] --
local function maybe_wake_parent_on_subtree_idle(keys, args)
    local task_statuses_key = keys[1]
    local task_agents_key = keys[2]
    local queue_main_key_template = keys[3]
    local queue_pending_key_template = keys[4]
    local activity_wait_meta_key = keys[5]
    local task_steering_key_template = keys[6]
    local message_seq_key = keys[7]
    local task_children_key_template = keys[8]
    local queue_scheduled_key_template = keys[9]
    local scheduled_wait_meta_key = keys[10]

    local parent_task_id = args[1]
    local source_task_id = args[2] or ""
    if not parent_task_id or parent_task_id == "" or parent_task_id == cjson.null then
        return false
    end

    if redis.call('HGET', task_statuses_key, parent_task_id) ~= "paused" then
        return false
    end
    if redis.call('HEXISTS', activity_wait_meta_key, parent_task_id) ~= 1 then
        return false
    end

    local children_key = _format_template_key(
        task_children_key_template,
        "{parent_task_id}",
        parent_task_id
    )
    local child_task_ids = redis.call('SMEMBERS', children_key)
    if #child_task_ids == 0 then
        return false
    end

    for _, child_task_id in ipairs(child_task_ids) do
        local child_status = redis.call('HGET', task_statuses_key, child_task_id)

        if child_status == "queued"
            or child_status == "active"
            or child_status == "processing"
            or child_status == "backoff"
            or child_status == "pending_tool_results"
            or child_status == "pending_child_tasks"
        then
            return false
        elseif child_status == "paused" then
            if redis.call('HEXISTS', activity_wait_meta_key, child_task_id) == 1 then
                -- child is paused(activity) and quiescent for subtree-idle checks
            else
                -- paused(sleep/cron) is still runnable via timer and not quiescent
                return false
            end
        elseif child_status == "completed"
            or child_status == "failed"
            or child_status == "cancelled"
            or not child_status
        then
            -- terminal/missing children are quiescent for subtree-idle purposes
        else
            return false
        end
    end

    local content = "<system_activity kind='subtree_idle' source_task_id='"
        .. tostring(source_task_id)
        .. "'></system_activity>"
    return wake_task_if_waiting_activity(
        {
            task_statuses_key,
            task_agents_key,
            queue_main_key_template,
            queue_pending_key_template,
            activity_wait_meta_key,
            task_steering_key_template,
            message_seq_key,
            queue_scheduled_key_template,
            scheduled_wait_meta_key,
        },
        {
            parent_task_id,
            content,
        }
    )
end


--[[
---------------------------------------------------------------------
Wake a parent waiting on activity when a direct child reaches terminal state.

Usage:
wake_parent_on_child_terminal(
    {
        task_statuses_key,
        task_agents_key,
        queue_main_key_template,
        queue_pending_key_template,
        activity_wait_meta_key,
        task_steering_key_template,
        message_seq_key,
        queue_scheduled_key_template, -- optional
        scheduled_wait_meta_key,      -- optional
    },
    {
        parent_task_id,
        child_task_id,
    }
)

Returns:
true when parent was woken, false otherwise.
---------------------------------------------------------------------
]] --
local function wake_parent_on_child_terminal(keys, args)
    local task_statuses_key = keys[1]
    local task_agents_key = keys[2]
    local queue_main_key_template = keys[3]
    local queue_pending_key_template = keys[4]
    local activity_wait_meta_key = keys[5]
    local task_steering_key_template = keys[6]
    local message_seq_key = keys[7]
    local queue_scheduled_key_template = keys[8]
    local scheduled_wait_meta_key = keys[9]

    local parent_task_id = args[1]
    local child_task_id = args[2] or ""
    if not parent_task_id or parent_task_id == "" or parent_task_id == cjson.null then
        return false
    end

    local content = "<system_activity kind='child_terminal' child_task_id='"
        .. tostring(child_task_id)
        .. "'></system_activity>"
    return wake_task_if_waiting_activity(
        {
            task_statuses_key,
            task_agents_key,
            queue_main_key_template,
            queue_pending_key_template,
            activity_wait_meta_key,
            task_steering_key_template,
            message_seq_key,
            queue_scheduled_key_template,
            scheduled_wait_meta_key,
        },
        {
            parent_task_id,
            content,
        }
    )
end
