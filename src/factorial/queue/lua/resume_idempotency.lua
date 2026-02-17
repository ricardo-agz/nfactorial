local resume_idempotency_key = KEYS[1]

local request_hash = ARGV[1]
local resumed_task_id = ARGV[2]
local source_task_id = ARGV[3]
local ttl_seconds = tonumber(ARGV[4]) or 0

if ttl_seconds < 1 then
    ttl_seconds = 1
end

local function persist_record()
    local record = {
        source_task_id = source_task_id,
        resumed_task_id = resumed_task_id,
        request_hash = request_hash,
    }
    redis.call('SET', resume_idempotency_key, cjson.encode(record), 'EX', ttl_seconds)
end

local existing_raw = redis.call('GET', resume_idempotency_key)
if not existing_raw then
    persist_record()
    return { 'claimed', resumed_task_id, request_hash }
end

local ok, existing = pcall(cjson.decode, existing_raw)
if not ok or type(existing) ~= 'table' then
    persist_record()
    return { 'claimed', resumed_task_id, request_hash }
end

local existing_hash = existing.request_hash
local existing_resumed_task_id = existing.resumed_task_id
if type(existing_hash) ~= 'string' or type(existing_resumed_task_id) ~= 'string' then
    persist_record()
    return { 'claimed', resumed_task_id, request_hash }
end

if existing_hash ~= request_hash then
    return { 'conflict', existing_resumed_task_id, existing_hash }
end

redis.call('EXPIRE', resume_idempotency_key, ttl_seconds)
return { 'replay', existing_resumed_task_id, existing_hash }
