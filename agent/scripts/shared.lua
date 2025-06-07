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
    { 'cancelled', pickups, retries, meta_json, metrics_ttl }
)

Returns:
true
---------------------------------------------------------------------
]] --
local function inc_metrics(keys, args)
    local agent_metrics_bucket_key  = keys[1]
    local global_metrics_bucket_key = keys[2]
    local metric_type               = args[1]
    local task_pickups              = args[2]
    local task_retries              = args[3]
    local task_meta_json            = args[4]
    local ttl                       = args[5]

    local time_result               = redis.call('TIME')
    local timestamp                 = tonumber(time_result[1]) + (tonumber(time_result[2]) / 1000000)

    local meta                      = cjson.decode(task_meta_json)
    local duration                  = timestamp - (tonumber(meta.created_at) or timestamp)
    local pickups                   = tonumber(task_pickups) or 0
    local retries                   = tonumber(task_retries) or 0
    local turns                     = pickups - retries

    local metric_count              = metric_type .. '_count'
    local metric_total_duration     = metric_type .. '_total_duration'
    local metric_total_turns        = metric_type .. '_total_turns'
    local metric_total_retries      = metric_type .. '_total_retries'
    local metric_total_pickups      = metric_type .. '_total_pickups'

    -- agent bucket ──────────────────────────────────────────────────────
    redis.call('HINCRBY', agent_metrics_bucket_key, metric_count, 1)
    redis.call('HINCRBYFLOAT', agent_metrics_bucket_key, metric_total_duration, duration)
    redis.call('HINCRBY', agent_metrics_bucket_key, metric_total_turns, turns)
    redis.call('HINCRBY', agent_metrics_bucket_key, metric_total_retries, retries)
    redis.call('HINCRBY', agent_metrics_bucket_key, metric_total_pickups, pickups)
    redis.call('EXPIRE', agent_metrics_bucket_key, ttl)
    -- global bucket ─────────────────────────────────────────────────────
    redis.call('HINCRBY', global_metrics_bucket_key, metric_count, 1)
    redis.call('HINCRBYFLOAT', global_metrics_bucket_key, metric_total_duration, duration)
    redis.call('HINCRBY', global_metrics_bucket_key, metric_total_turns, turns)
    redis.call('HINCRBY', global_metrics_bucket_key, metric_total_retries, retries)
    redis.call('HINCRBY', global_metrics_bucket_key, metric_total_pickups, pickups)
    redis.call('EXPIRE', global_metrics_bucket_key, ttl)

    return true
end
