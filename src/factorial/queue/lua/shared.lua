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
