-- harness/context_test.lua

print("[LUA] Loading context_test harness")

function on_before_inference(ctx)
    print("\n[LUA] >>> Hook: on_before_inference triggered <<<")
    
    -- 1. Read Properties
    local model = ctx.model
    print("[LUA] Model: " .. tostring(model))
    
    local sys = ctx.system_prompt
    print("[LUA] Original System Prompt: " .. sys)
    
    local count = ctx.token_count
    print("[LUA] Token Count: " .. count)

    -- 2. Modify System Prompt
    ctx.system_prompt = sys .. "\n\n[Verified by context_test.lua]"
    print("[LUA] Updated System Prompt (appended verification tag)")
    
    -- 3. Inspect Messages
    local msgs = ctx.messages
    print("[LUA] Message count: " .. #msgs)
    
    for i, msg in ipairs(msgs) do
        -- msg.content is array of blocks
        local txt = ""
        if msg.content and msg.content[1] and msg.content[1].text then
             txt = msg.content[1].text
        end
        print("[LUA] Msg " .. i .. " (" .. msg.role .. "): " .. string.sub(txt, 1, 50) .. "...")
    end
    
    -- 4. Inject Message
    -- Note: InferenceRole is strict (User/Assistant), so we inject as user for now.
    ctx:add_message({
        role = "user",
        content = {{type="text", text="[Injection from Lua Hook]"}}
    })
    print("[LUA] Added injected user message")
    
    print("[LUA] >>> Hook finished <<<")
    return {type="allow"}
end
