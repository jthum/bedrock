-- budget.lua: Enforce a token budget per session
local BUDGET_LIMIT = 50000

function on_token_usage(usage)
    local used = usage.total_tokens
    
    -- Log usage for visibility
    if used > (BUDGET_LIMIT * 0.8) then
        log(string.format("Warning: Session is at %d%% of token budget (%d/%d)", 
            (used / BUDGET_LIMIT) * 100, used, BUDGET_LIMIT))
    end

    -- Update state (persisted across restarts)
    db.kv_set("session_tokens", tostring(used))
    
    return ALLOW
end

function on_tool_call(call)
    -- Check if we've exceeded budget
    local used_str = db.kv_get("session_tokens")
    local used = tonumber(used_str) or 0
    
    if used >= BUDGET_LIMIT then
        return REJECT, string.format("Token budget exceeded (%d/%d). Payment required to continue.", used, BUDGET_LIMIT)
    end
    
    return ALLOW
end
