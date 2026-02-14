-- priority_test.lua
-- Verifies session.queue_next (priority injection) functionality

function on_agent_start(ev)
    print("\n[Harness] on_agent_start: Setting up test queue")
    
    -- Add a normal task to the end (Task 3)
    session.queue("Say 'Task 3 (Normal Queue)'")
    
    -- Add a priority task (Task 2) - should run BEFORE Task 3
    session.queue_next("Say 'Task 2 (Priority Injection)'")
    
    -- The initial prompt "Task 1" is running now.
    -- Expected Order: Task 1 -> Task 2 -> Task 3
    return ALLOW
end
