-- decomposition_test.lua
-- Verifies that the harness can intercept and modify agent plans.

function on_task_submit(payload)
    print("[HARNESS] Intercepted submit_task: " .. payload.title)
    
    if payload.title == "Test Plan" then
        print("[HARNESS] Modifying 'Test Plan'...")
        -- Override the agent's plan with our own
        return MODIFY, { "Modified Task A", "Modified Task B" }
    elseif payload.title == "Forbidden Plan" then
        print("[HARNESS] Rejecting 'Forbidden Plan'...")
        return REJECT, "Planning is forbidden for this topic."
    end

    return ALLOW
end

-- Log when the modified tasks are actually executed
function on_before_inference(ctx)
    local msgs = ctx:get_messages()
    local latest = msgs[#msgs]

    if latest and latest.role == "user" then
        -- Simple heuristic: if the user asks for a "plan" or a "website", force decomposition
        local text = latest.content[1].text:lower()
        if text:find("plan") or text:find("website") or text:find("complex") then
            print("[HARNESS] Steering agent toward decomposition...")
            local current = ctx:get_system_prompt()
            ctx:set_system_prompt(current .. "\n\nYour first step for this task MUST be to use 'submit_task' to break it down.")
        end
    end
    return ALLOW
end
