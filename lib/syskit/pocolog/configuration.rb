module Syskit::Pocolog
    # Extension of the Syskit configuration class to add APIs related to
    # replaying tasks
    module Configuration
        # Expose a given set of streams as a task context in Syskit
        def use_pocolog_task(streams, name: streams.task_name, model: streams.replay_model, allow_missing: true, on: 'pocolog')

            deployment_model = Deployment.new_submodel(task_name: name, task_model: model,
                                                       name: "Deployment::Pocolog::#{name}")
            deployment_model.add_streams_from(streams, allow_missing: allow_missing)

            if !has_process_server?(on)
                mng = Orocos::RubyTasks::ProcessManager.new(app.default_loader)
                register_process_server(on, mng, app.log_dir)
            end

            configured_deployment = Syskit::Models::ConfiguredDeployment.
                new(on, deployment_model, Hash[name => name], name, Hash.new)
            register_configured_deployment(configured_deployment)
            configured_deployment
        end
    end
    Syskit::RobyApp::Configuration.include Configuration
end

