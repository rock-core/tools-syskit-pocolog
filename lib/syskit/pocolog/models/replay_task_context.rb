module Syskit
    module Pocolog
        module Models
            # Model of tasks that replay data streams
            #
            # To replay the data streams in a Syskit network, one cannot use the
            # normal Syskit::TaskContext tasks, as they can be customized by the
            # system designer (reimplement #configure, add polling blocks,
            # scripts, ...)
            #
            # So, instead, syskit-pocolog maintains a parallel hierarchy of task
            # context models that mirrors the "plain" ones, but does not have
            # all the runtime handlers
            module ReplayTaskContext
                # The corresponding "plain" task context model (from
                # {Syskit::TaskContext}
                attr_accessor :plain_task_context

                # Returns the {ReplayTaskContext} model that should be used to
                # replay tasks of the given orogen model
                def model_for(orogen_model)
                    if model = find_model_by_orogen(orogen_model)
                        model
                    else
                        define_from_orogen(orogen_model, register: true)
                    end
                end

                def register_syskit_model_from_orogen_name(model)
                    orogen_model = model.orogen_model
                    namespace, basename = syskit_names_from_orogen_name(orogen_model.name)
                    register_syskit_model(OroGen::Pocolog, namespace, basename, model)
                end

                # @api private
                #
                # Setup a newly created {ReplayTaskContext}. This is called
                # internally by MetaRuby's #new_submodel
                def setup_submodel(submodel, **options, &block)
                    super

                    # We want to "copy" the services (dynamic and plain) from
                    # the plain model
                    if plain_model = Syskit::TaskContext.find_model_by_orogen(submodel.orogen_model)
                        submodel.instance_variable_set :@plain_task_context, plain_model
                        submodel.copy_services_from_plain_model(plain_model)
                    else
                        submodel.instance_variable_set :@plain_task_context, Syskit::TaskContext
                    end
                end

                # @api private
                #
                # Copy the services of a task model (in this case, expected to
                # be the replay model's {#plain_task_context}) onto this model
                def copy_services_from_plain_model(plain_model)
                    plain_model.each_data_service do |name, srv|
                        data_services[name] = srv.attach(self)
                    end
                    plain_model.each_dynamic_service do |name, srv|
                        dynamic_services[name] = srv.attach(self)
                    end
                end


                # Reimplemented to make ReplayTaskContext fullfills?
                # {#plain_task_context}
                def fullfills?(model)
                    super || plain_task_context.fullfills?(model)
                end
            end
        end
    end
end

