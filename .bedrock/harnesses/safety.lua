function on_tool_call(call)
    if call.name == "shell_exec" then
        local cmd = call.args.command
        if cmd and cmd:find("rm %-rf") then
            return REJECT, "Destructive command 'rm -rf' is not allowed by safety policy."
        end
    end
    return ALLOW
end

function on_token_usage(usage)
    if usage.total_tokens > 10000 then
        log("Warning: High token usage detected in this session!")
    end
    return ALLOW
end
