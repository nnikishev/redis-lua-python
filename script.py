
lua_reservation_script = """
local available_key = KEYS[1]
local reserved_key = KEYS[2]
local required_amount = tonumber(ARGV[1])
if redis.call("EXISTS", available_key) == 0 or redis.call("EXISTS", reserved_key) == 0 then
    return {1, false}
end
local current_available = tonumber(redis.call("GET", available_key))
if current_available >= required_amount then
    redis.call("INCRBY", available_key, -required_amount)
    redis.call("INCRBY", reserved_key, required_amount)
    local current_available = tonumber(redis.call("GET", available_key))
    return {2, current_available}
else
    return {3, current_available}
end
"""