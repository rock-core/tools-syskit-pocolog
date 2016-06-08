module Syskit::Pocolog
    module Plugin
        def self.setup(app)
            manager = Orocos::RubyTasks::ProcessManager.new(app.default_loader)
            Syskit.conf.register_process_server('pocolog', manager, app.log_dir)
        end
    end
end

