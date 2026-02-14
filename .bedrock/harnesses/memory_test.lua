-- memory_test.lua
-- Verifies that the agent can store and retrieve semantic memories.

local function test_memory_flow()
    log("Starting memory flow test...")

    -- 1. Store some facts
    log("Storing facts...")
    local facts = {
        "The project codename is Project Bedrock.",
        "The lead developer is Jane Doe.",
        "We are using Rust for the kernel implementation.",
        "The database is powered by Turso and libSQL."
    }

    for _, fact in ipairs(facts) do
        local ok, err = bedrock.memory.store(fact, { source = "test_script", timestamp = time.now_utc() })
        if not ok then
            log("Failed to store fact: " .. fact .. " Error: " .. tostring(err))
            return false
        end
    end
    log("Facts stored successfully.")

    -- 2. Search for related information
    log("Searching for 'who is the lead dev'...")
    local results = bedrock.memory.search("who is the lead dev", 1)
    
    if #results == 0 then
        log("No results found!")
        return false
    end

    local best_match = results[1]
    log("Top match: " .. best_match.content .. " (Score: " .. best_match.score .. ")")

    if string.find(best_match.content, "Jane Doe") then
        log("SUCCESS: Retrieved correct fact about lead developer.")
    else
        log("FAILURE: Retrieved irrelevant fact.")
        return false
    end

    -- 3. Search for technical details
    log("Searching for 'database technology'...")
    local tech_results = bedrock.memory.search("database technology", 1)
    if #tech_results > 0 then
        log("Top match: " .. tech_results[1].content)
        if string.find(tech_results[1].content, "Turso") then
            log("SUCCESS: Retrieved correct fact about database.")
            return true
        end
    end
    
    log("FAILURE: Could not find database fact.")
    return false
end

-- Run the test
if test_memory_flow() then
    log("MEMORY TEST PASSED")
else
    log("MEMORY TEST FAILED")
end
