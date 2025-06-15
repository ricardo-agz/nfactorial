local queue_main_key = KEYS[1]
local queue_failed_key = KEYS[2]
local queue_orphaned_key = KEYS[3]
local task_statuses_key = KEYS[4]
local task_agents_key = KEYS[5]
local task_payloads_key = KEYS[6]
local task_pickups_key = KEYS[7]
local task_retries_key = KEYS[8]
local task_metas_key = KEYS[9]
local processing_heartbeats_key = KEYS[10]
local agent_metrics_bucket_key = KEYS[11]
local global_metrics_bucket_key = KEYS[12]

local cutoff_timestamp = tonumber(ARGV[1])
local max_recovery_batch = tonumber(ARGV[2])
local max_retries = tonumber(ARGV[3])
local metrics_ttl = tonumber(ARGV[4])


-- Get stale processing keys (limited batch to avoid long-running operations)
local stale_task_ids = redis.call('ZRANGEBYSCORE', processing_heartbeats_key, 0, cutoff_timestamp, 'LIMIT', 0,
    max_recovery_batch)

local recovered_count = 0
local failed_count = 0
local time_result = redis.call('TIME')
local curr_timestamp = tonumber(time_result[1]) + (tonumber(time_result[2]) / 1000000)
local stale_task_actions = {}

for i = 1, #stale_task_ids do
    local task_id = stale_task_ids[i]

    local task_result = load_task(
        { task_statuses_key, task_agents_key, task_payloads_key, task_pickups_key, task_retries_key, task_metas_key },
        { task_id }
    )

    local status = task_result.status
    local pickups = task_result.pickups
    local retries = task_result.retries
    local meta_json = task_result.meta

    redis.call('ZREM', processing_heartbeats_key, task_id)

    -- Only recover if:
    -- 1. Task is in a valid state for recovery, not missing or corrupted
    -- 2. Task is still processing
    -- 3. Task has not exceeded the max number of retries
    if task_result.state == "ok" then
        if status == "processing" then
            if retries < max_retries then
                -- Increment retry count and put back in queue and set status to active
                redis.call('HINCRBY', task_retries_key, task_id, 1)
                redis.call('HSET', task_statuses_key, task_id, "active")
                redis.call('LPUSH', queue_main_key, task_id)
                table.insert(stale_task_actions, { task_id, "recovered" })
                recovered_count = recovered_count + 1

                -- Update timeline metrics for recovery
                inc_metrics(
                    { agent_metrics_bucket_key, global_metrics_bucket_key },
                    { 'recovery', pickups, retries, meta_json, metrics_ttl }
                )
            else
                -- Max retries exceeded, send to failed queue and set status to failed
                redis.call('ZADD', queue_failed_key, curr_timestamp, task_id)
                redis.call('HSET', task_statuses_key, task_id, "failed")
                table.insert(stale_task_actions, { task_id, "failed" })
                failed_count = failed_count + 1

                -- Update timeline metrics
                inc_metrics(
                    { agent_metrics_bucket_key, global_metrics_bucket_key },
                    { 'failed', pickups, retries, meta_json, metrics_ttl }
                )
            end
        end
    elseif task_result.state == "missing" then
        -- Task is missing, add to orphaned queue
        redis.call('ZADD', queue_orphaned_key, curr_timestamp, task_id)
        table.insert(stale_task_actions, { task_id, "orphaned" })
    elseif task_result.state == "corrupted" then
        -- Task data is corrupted, just remove from processing and ignore
        table.insert(stale_task_actions, { task_id, "corrupted" })
    end
end

return { recovered_count, failed_count, stale_task_actions }
