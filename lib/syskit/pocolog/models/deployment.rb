module Syskit::Pocolog
    module Models
        # Metamodel for {Syskit::Pocolog::Deployment}
        module Deployment
            # Pocolog deployments only have a single task. This is it
            attr_reader :task_model

            # Mappings from streams to ports
            attr_reader :streams_to_port

            def new_submodel(task_name: nil, task_model: nil, **options, &block)
                super(**options) do
                    task task_name, task_model.orogen_model
                end
            end

            def setup_submodel(submodel, **options, &block)
                super
                orogen_model = submodel.each_orogen_deployed_task_context_model.first
                submodel.instance_variable_set :@task_model, Syskit::TaskContext.model_for(orogen_model.task_model)
                submodel.instance_variable_set :@streams_to_port, Hash.new
            end

            # Add all matching streams from the given streams argument
            #
            # The streams are matched by name. 
            #
            # @param [Streams] streams the set of streams that should be
            #   exported. Streams which does not have a corresponding output port
            #   are ignored.
            # @param [Boolean] allow_missing controls the behaviour if the task
            #   has a port that does not have a matching stream. If true, a
            #   warning is issued, if false a {MissingStream} exception is raised.
            #
            #   Note that this does not affect the error raised if a task's
            #   output is connected with no associated stream
            def add_streams_from(streams, allow_missing: true)
                task_model.each_output_port do |p|
                    p_stream = streams.find_port_by_name(p.name)
                    if !p_stream
                        if allow_missing
                            Syskit::Pocolog.warn "no log stream available for #{p}, ignored as allow_missing is true"
                        else
                            raise MissingStream, "no stream named #{p.name}"
                        end
                    else
                        add_stream(p_stream, p)
                    end
                end
            end

            # Add a stream-to-port mapping
            def add_stream(stream, port = task_model.port_by_name(stream.metadata['rock_task_object_name']))
                if !port.output?
                    raise ArgumentError, "cannot map a log stream to an input port"
                elsif port.component_model != task_model
                    raise ArgumentError, "#{self} deploys #{task_model} but the stream mapping is for #{port.component_model}"
                elsif port.type != stream.type
                    raise MismatchingType.new(stream, port)
                end

                streams_to_port[stream] = port
            end
        end
    end
end



