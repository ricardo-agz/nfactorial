-- Metrics Aggregation Script (MAXIMUM OPTIMIZATION)
-- Collect all timeline metrics with absolute minimum Redis calls
--
-- KEYS[1] = timeline_duration (seconds)
-- KEYS[2] = bucket_duration (seconds)
-- KEYS[3] = agent_names_json (JSON array of agent names)
--
-- Returns: JSON string with all aggregated metrics

local timeline_duration = tonumber(KEYS[1])
local bucket_duration = tonumber(KEYS[2])
local agent_names = cjson.decode(KEYS[3])
local time_result = redis.call('TIME')
local timestamp = tonumber(time_result[1]) + (tonumber(time_result[2]) / 1000000)

local start_time = timestamp - timeline_duration
local start_bucket = math.floor(start_time / bucket_duration) * bucket_duration
local end_bucket = math.floor(timestamp / bucket_duration) * bucket_duration

local result = {
    system_totals = { queued = 0, processing = 0, idle = 0, backoff = 0 },
    activity_timeline = {
        completed = { buckets = {}, total_tasks = 0, total_duration = 0 },
        failed = { buckets = {}, total_tasks = 0, total_duration = 0 },
        cancelled = { buckets = {}, total_tasks = 0, total_duration = 0 },
        retried = { buckets = {}, total_tasks = 0, total_duration = 0 }
    },
    agent_metrics = {},
    agent_activity_timelines = {}
}

-- Step 1: Collect current queue states (unavoidable per-agent calls)
for _, agent_name in ipairs(agent_names) do
    local queued = redis.call('LLEN', 'queue:' .. agent_name .. ':main') or 0
    local processing = redis.call('ZCARD', 'processing:' .. agent_name .. ':heartbeats') or 0
    local backoff = redis.call('ZCARD', 'queue:' .. agent_name .. ':backoff') or 0
    local idle_data = redis.call('HGET', 'metrics:gauge:' .. agent_name .. ':idle', 'idle') or 0
    local last_heartbeat_data = redis.call('ZRANGE', 'processing:' .. agent_name .. ':heartbeats', -1, -1, 'WITHSCORES')
    local last_heartbeat = (#last_heartbeat_data > 0) and tonumber(last_heartbeat_data[2]) or nil

    result.agent_metrics[agent_name] = {
        queued = tonumber(queued),
        processing = tonumber(processing),
        backoff = tonumber(backoff),
        idle = tonumber(idle_data),
        last_heartbeat = last_heartbeat,
        completed = 0,
        failed = 0,
        total_duration = 0
    }

    -- Initialize per-agent activity timelines
    result.agent_activity_timelines[agent_name] = {
        completed = { buckets = {}, total_tasks = 0, total_duration = 0 },
        failed = { buckets = {}, total_tasks = 0, total_duration = 0 },
        cancelled = { buckets = {}, total_tasks = 0, total_duration = 0 },
        retried = { buckets = {}, total_tasks = 0, total_duration = 0 }
    }

    result.system_totals.queued = result.system_totals.queued + tonumber(queued)
    result.system_totals.processing = result.system_totals.processing + tonumber(processing)
    result.system_totals.backoff = result.system_totals.backoff + tonumber(backoff)
    result.system_totals.idle = result.system_totals.idle + tonumber(idle_data)
end

-- Step 2: Build bucket timestamps
local bucket_timestamps = {}
local bucket_time = start_bucket
while bucket_time <= end_bucket do
    table.insert(bucket_timestamps, bucket_time)
    bucket_time = bucket_time + bucket_duration
end

-- Step 3: Process all metrics in a single pass per bucket (minimizing Redis calls)
for _, bucket_timestamp in ipairs(bucket_timestamps) do
    -- Get global metrics for this bucket (4 calls per bucket - unavoidable)
    local completed_data = redis.call('HMGET', 'metrics:timeline:__all__:completed:' .. bucket_timestamp,
        'count', 'total_duration', 'total_turns', 'total_retries')
    local failed_data = redis.call('HMGET', 'metrics:timeline:__all__:failed:' .. bucket_timestamp,
        'count', 'total_duration', 'total_turns', 'total_retries')
    local cancelled_data = redis.call('HMGET', 'metrics:timeline:__all__:cancelled:' .. bucket_timestamp,
        'count', 'total_duration', 'total_turns', 'total_retries')
    local retried_data = redis.call('HMGET', 'metrics:timeline:__all__:retried:' .. bucket_timestamp,
        'count', 'total_duration', 'total_turns', 'total_retries')

    -- Process completed metrics
    local completed_count = tonumber(completed_data[1]) or 0
    local completed_duration = tonumber(completed_data[2]) or 0
    local completed_turns = tonumber(completed_data[3]) or 0
    local completed_retries = tonumber(completed_data[4]) or 0

    table.insert(result.activity_timeline.completed.buckets, {
        timestamp = bucket_timestamp,
        count = completed_count,
        total_duration = completed_duration,
        total_turns = completed_turns,
        total_retries = completed_retries
    })
    result.activity_timeline.completed.total_tasks = result.activity_timeline.completed.total_tasks + completed_count
    result.activity_timeline.completed.total_duration = result.activity_timeline.completed.total_duration +
        completed_duration

    -- Process failed metrics
    local failed_count = tonumber(failed_data[1]) or 0
    local failed_duration = tonumber(failed_data[2]) or 0
    local failed_turns = tonumber(failed_data[3]) or 0
    local failed_retries = tonumber(failed_data[4]) or 0

    table.insert(result.activity_timeline.failed.buckets, {
        timestamp = bucket_timestamp,
        count = failed_count,
        total_duration = failed_duration,
        total_turns = failed_turns,
        total_retries = failed_retries
    })
    result.activity_timeline.failed.total_tasks = result.activity_timeline.failed.total_tasks + failed_count
    result.activity_timeline.failed.total_duration = result.activity_timeline.failed.total_duration + failed_duration

    -- Process cancelled metrics
    local cancelled_count = tonumber(cancelled_data[1]) or 0
    local cancelled_duration = tonumber(cancelled_data[2]) or 0
    local cancelled_turns = tonumber(cancelled_data[3]) or 0
    local cancelled_retries = tonumber(cancelled_data[4]) or 0

    table.insert(result.activity_timeline.cancelled.buckets, {
        timestamp = bucket_timestamp,
        count = cancelled_count,
        total_duration = cancelled_duration,
        total_turns = cancelled_turns,
        total_retries = cancelled_retries
    })
    result.activity_timeline.cancelled.total_tasks = result.activity_timeline.cancelled.total_tasks + cancelled_count
    result.activity_timeline.cancelled.total_duration = result.activity_timeline.cancelled.total_duration +
        cancelled_duration

    -- Process retried metrics
    local retried_count = tonumber(retried_data[1]) or 0
    local retried_duration = tonumber(retried_data[2]) or 0
    local retried_turns = tonumber(retried_data[3]) or 0
    local retried_retries = tonumber(retried_data[4]) or 0

    table.insert(result.activity_timeline.retried.buckets, {
        timestamp = bucket_timestamp,
        count = retried_count,
        total_duration = retried_duration,
        total_turns = retried_turns,
        total_retries = retried_retries
    })
    result.activity_timeline.retried.total_tasks = result.activity_timeline.retried.total_tasks + retried_count
    result.activity_timeline.retried.total_duration = result.activity_timeline.retried.total_duration + retried_duration

    -- Get per-agent metrics for this bucket (4 calls per agent per bucket for all metric types)
    for _, agent_name in ipairs(agent_names) do
        local agent_completed_data = redis.call('HMGET',
            'metrics:timeline:' .. agent_name .. ':completed:' .. bucket_timestamp, 'count', 'total_duration',
            'total_turns', 'total_retries')
        local agent_failed_data = redis.call('HMGET',
            'metrics:timeline:' .. agent_name .. ':failed:' .. bucket_timestamp, 'count', 'total_duration', 'total_turns',
            'total_retries')
        local agent_cancelled_data = redis.call('HMGET',
            'metrics:timeline:' .. agent_name .. ':cancelled:' .. bucket_timestamp, 'count', 'total_duration',
            'total_turns', 'total_retries')
        local agent_retried_data = redis.call('HMGET',
            'metrics:timeline:' .. agent_name .. ':retried:' .. bucket_timestamp, 'count', 'total_duration',
            'total_turns', 'total_retries')

        -- Process agent completed metrics
        local agent_completed_count = tonumber(agent_completed_data[1]) or 0
        local agent_completed_duration = tonumber(agent_completed_data[2]) or 0
        local agent_completed_turns = tonumber(agent_completed_data[3]) or 0
        local agent_completed_retries = tonumber(agent_completed_data[4]) or 0

        table.insert(result.agent_activity_timelines[agent_name].completed.buckets, {
            timestamp = bucket_timestamp,
            count = agent_completed_count,
            total_duration = agent_completed_duration,
            total_turns = agent_completed_turns,
            total_retries = agent_completed_retries
        })
        result.agent_activity_timelines[agent_name].completed.total_tasks =
            result.agent_activity_timelines[agent_name].completed.total_tasks + agent_completed_count
        result.agent_activity_timelines[agent_name].completed.total_duration =
            result.agent_activity_timelines[agent_name].completed.total_duration + agent_completed_duration

        -- Process agent failed metrics
        local agent_failed_count = tonumber(agent_failed_data[1]) or 0
        local agent_failed_duration = tonumber(agent_failed_data[2]) or 0
        local agent_failed_turns = tonumber(agent_failed_data[3]) or 0
        local agent_failed_retries = tonumber(agent_failed_data[4]) or 0

        table.insert(result.agent_activity_timelines[agent_name].failed.buckets, {
            timestamp = bucket_timestamp,
            count = agent_failed_count,
            total_duration = agent_failed_duration,
            total_turns = agent_failed_turns,
            total_retries = agent_failed_retries
        })
        result.agent_activity_timelines[agent_name].failed.total_tasks =
            result.agent_activity_timelines[agent_name].failed.total_tasks + agent_failed_count
        result.agent_activity_timelines[agent_name].failed.total_duration =
            result.agent_activity_timelines[agent_name].failed.total_duration + agent_failed_duration

        -- Process agent cancelled metrics
        local agent_cancelled_count = tonumber(agent_cancelled_data[1]) or 0
        local agent_cancelled_duration = tonumber(agent_cancelled_data[2]) or 0
        local agent_cancelled_turns = tonumber(agent_cancelled_data[3]) or 0
        local agent_cancelled_retries = tonumber(agent_cancelled_data[4]) or 0

        table.insert(result.agent_activity_timelines[agent_name].cancelled.buckets, {
            timestamp = bucket_timestamp,
            count = agent_cancelled_count,
            total_duration = agent_cancelled_duration,
            total_turns = agent_cancelled_turns,
            total_retries = agent_cancelled_retries
        })
        result.agent_activity_timelines[agent_name].cancelled.total_tasks =
            result.agent_activity_timelines[agent_name].cancelled.total_tasks + agent_cancelled_count
        result.agent_activity_timelines[agent_name].cancelled.total_duration =
            result.agent_activity_timelines[agent_name].cancelled.total_duration + agent_cancelled_duration

        -- Process agent retried metrics
        local agent_retried_count = tonumber(agent_retried_data[1]) or 0
        local agent_retried_duration = tonumber(agent_retried_data[2]) or 0
        local agent_retried_turns = tonumber(agent_retried_data[3]) or 0
        local agent_retried_retries = tonumber(agent_retried_data[4]) or 0

        table.insert(result.agent_activity_timelines[agent_name].retried.buckets, {
            timestamp = bucket_timestamp,
            count = agent_retried_count,
            total_duration = agent_retried_duration,
            total_turns = agent_retried_turns,
            total_retries = agent_retried_retries
        })
        result.agent_activity_timelines[agent_name].retried.total_tasks =
            result.agent_activity_timelines[agent_name].retried.total_tasks + agent_retried_count
        result.agent_activity_timelines[agent_name].retried.total_duration =
            result.agent_activity_timelines[agent_name].retried.total_duration + agent_retried_duration

        -- Update agent summary metrics
        result.agent_metrics[agent_name].completed = result.agent_metrics[agent_name].completed + agent_completed_count
        result.agent_metrics[agent_name].total_duration = result.agent_metrics[agent_name].total_duration +
            agent_completed_duration
        result.agent_metrics[agent_name].failed = result.agent_metrics[agent_name].failed + agent_failed_count
    end
end

return cjson.encode(result)
