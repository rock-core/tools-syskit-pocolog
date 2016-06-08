module Syskit::Pocolog
    module Extensions
        # Extension of the Syskit::DeploymentGroup class to add APIs related to
        # replaying tasks
        module DeploymentGroup
            # Expose a given set of streams as a task context in Syskit
            def use_pocolog_task(streams, name: streams.task_name, model: streams.replay_model, allow_missing: true, on: 'pocolog', process_managers: Syskit.conf)
                # Verify the process manager's availability
                process_managers.process_server_config_for(on)

                deployment_model = Deployment.new_submodel(task_name: name, task_model: model,
                                                           name: "Deployment::Pocolog::#{name}")
                deployment_model.add_streams_from(streams, allow_missing: allow_missing)

                configured_deployment = Syskit::Models::ConfiguredDeployment.
                    new(on, deployment_model, Hash[name => name], name, Hash.new)
                register_configured_deployment(configured_deployment)
                configured_deployment
            end
        end
    end
end

