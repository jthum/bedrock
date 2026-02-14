-- queue_test.lua
-- Verifies session.queue functionality

function on_agent_start(ev)
    print("\n[Harness] on_agent_start: Queuing follow-up task")
    session.queue("Say 'Second Task'")
    return ALLOW
end

function on_tool_call(call)
    return ALLOW
end
