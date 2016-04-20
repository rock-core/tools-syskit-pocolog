module Syskit::Pocolog
    module ExecutionEngineExtension
        def pocolog_replay_manager
            @pocolog_replay_manager ||= ReplayManager.new
        end
    end

    Roby::ExecutionEngine.include ExecutionEngineExtension
end

