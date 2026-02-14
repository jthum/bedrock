function on_before_inference(ctx)
    log(">>> coding_agent.lua: on_before_inference <<<")

    -- 1. Project Instructions
    if fs.exists("BEDROCK.md") then
        local instructions = fs.read("BEDROCK.md")
        if instructions then
            log("Injecting BEDROCK.md (" .. #instructions .. " bytes)")
            -- Append to system prompt
            ctx.system_prompt = ctx.system_prompt .. "\n\n=== Project Instructions ===\n" .. instructions
        else
            log("Failed to read BEDROCK.md")
        end
    else
        log("BEDROCK.md not found")
    end

    -- 2. Automatic Memory Recall
    -- If the memory module is available, search for relevant facts.
    if bedrock.memory and bedrock.memory.search then
        -- Get the last user message to use as a query
        local last_message = nil
        -- Context is dynamic, we need to inspect the messages. 
        -- But `ctx` exposed here is a UserData wrapper with methods, not a table.
        -- We don't have direct access to messages list via `ctx` efficiently yet?
        -- Wait, `on_before_inference` receives `ContextWrapper`.
        -- Let's look at `src/harness/context.rs` to see what `ContextWrapper` exposes.
        -- It exposes `messages()` which returns a table of messages.
        
        local messages = ctx.messages
        if messages and #messages > 0 then
            -- Find last user message
            for i = #messages, 1, -1 do
                if messages[i].role == "user" then
                    last_message = messages[i].content
                    break
                end
            end
        end

        if last_message then
             -- We only query if the message is substantial
             if type(last_message) == "string" and #last_message > 10 then
                 log("Searching memories for: " .. string.sub(last_message, 1, 50) .. "...")
                 local status, memories = pcall(bedrock.memory.search, last_message, 3)
                 if status and memories and #memories > 0 then
                     log("Recalled " .. #memories .. " memories.")
                     local memory_block = "\n\n=== Relevant Memories ===\n"
                     for _, mem in ipairs(memories) do
                         memory_block = memory_block .. "- " .. mem.content .. " (Confidence: " .. string.format("%.2f", mem.score) .. ")\n"
                     end
                     ctx.system_prompt = ctx.system_prompt .. memory_block
                 else
                     log("No relevant memories found or search failed.")
                 end
             end
        end
    end

    -- 2. Summarization Test
    -- We try to summarize the current messages.
    -- With 'dummy' key, this should fail gracefully.
    log("Attempting summarization (expecting failure with dummy key)...")
    
    -- pcall is safer
    local status, result = pcall(function() 
        return ctx:summarize() 
    end)

    if status then
        if result then
            log("Summarization success! (Unexpected with dummy key)")
            log("Summary: " .. string.sub(result, 1, 50) .. "...")
        else
            log("Summarization returned nil (or None)")
        end
    else
        log("Summarization failed as expected: " .. tostring(result))
    end

    return ALLOW
end


function on_task_submit(event)
    log(">>> coding_agent.lua: on_task_submit <<<")
    -- event contains the tool arguments (subtasks)
    return ALLOW
end

function on_task_complete(event)
    log(">>> coding_agent.lua: on_task_complete <<<")
    local session_id = event.session_id
    
    -- Anchorage: Summarize and store session learnings
    if bedrock.session and bedrock.session.load and bedrock.memory and bedrock.memory.store and bedrock.agent and bedrock.agent.spawn then
        log("Analyzing session for anchorage...")
        local history = bedrock.session.load(session_id)
        if history and #history > 0 then
            -- Construct a transcript
            local transcript = ""
            for _, msg in ipairs(history) do
                local role = msg.role
                local content = ""
                -- Handle different content types (schema is confusing, lets be safe)
                -- msg.content is likely a Lua table (JSON array) or string
                if type(msg.content) == "table" then
                     for _, block in ipairs(msg.content) do
                         if block.type == "text" then
                             content = content .. (block.text or "")
                         elseif block.type == "tool_use" then
                             content = content .. "[Tool Use: " .. (block.name or "unknown") .. "]"
                         elseif block.type == "tool_result" then
                             content = content .. "[Tool Result: " .. (string.sub(tostring(block.content), 1, 50)) .. "...]"
                         end
                     end
                elseif type(msg.content) == "string" then
                    content = msg.content
                end
                
                transcript = transcript .. role:upper() .. ": " .. content .. "\n"
            end
            
            if #transcript > 50 then
                local prompt = "Summarize the following coding session into a single concise sentence focusing on technical facts established, bugs fixed, or architectural decisions made. Do not mention 'User' or 'Assistant'.:\n\n" .. transcript
                
                -- Spawn subagent
                log("Spawning summarizer agent...")
                local summary, err = bedrock.agent.spawn(prompt, {
                    system_prompt = "You are a concise technical summarizer.",
                    max_turns = 1
                })
                
                if summary and summary ~= "" then
                    log("ANCHOR: " .. summary)
                    -- Store in memory
                    local status, err = pcall(bedrock.memory.store, summary, { session_id = session_id, type = "anchorage" })
                    if not status then
                        log("Failed to store memory: " .. tostring(err))
                    else
                        log("Memory stored successfully.")
                    end
                else
                    log("Summarizer returned empty or error: " .. tostring(err))
                end
            else
                log("Transcript too short for summarization.")
            end
        else
            log("No history loaded.")
        end
    end

    return ALLOW
end
