module Syskit
    module Pocolog
        # Tasks that replay data streams
        #
        # To replay the data streams in a Syskit network, one cannot use the
        # normal Syskit::TaskContext tasks, as they can be customized by the
        # system designer (reimplement {#configure}, add polling blocks,
        # scripts, ...)
        #
        # So, instead, syskit-pocolog maintains a parallel hierarchy of task
        # context models that mirrors the "plain" ones, but does not have all
        # the runtime handlers
        class ReplayTaskContext < TaskContext
            extend Models::ReplayTaskContext
        end
    end
end

