module Syskit::Pocolog
    # The object that manages the replay itself
    #
    # Deployments register and deregister themselves when started/stopped.
    #
    # There is one per execution engine, which is accessible via
    # {ExecutionEngineExtension#pocolog_replay_manager}. The way to add/remove
    # deployment tasks is through {.register} and {.deregister}, which are
    # already automatically called on the deployment's start/stop events.
    class ReplayManager
        # The underlying stream aligner
        # 
        # @return [Pocolog::StreamAligner]
        attr_reader :stream_aligner

        # The set of streams aligned so far, with how many deployment tasks are
        # referring to them (for cleanup)
        #
        # @return [Hash<Pocolog::DataStream,Set<Deployment>>]
        attr_reader :stream_to_deployment

        def initialize
            @stream_aligner = Pocolog::StreamAligner.new(false)
            @stream_to_deployment = Hash.new { |h, k| h[k] = Set.new }
        end

        # Register a deployment task
        #
        # @param [Deployment] deployment_task the task to register
        # @return [void]
        def register(deployment_task)
            new_streams = Array.new
            deployment_task.model.each_stream_mapping do |s, _|
                set = (stream_to_deployment[s] << deployment_task)
                if set.size == 1
                    new_streams << s
                end
            end
            stream_aligner.add_streams(*new_streams)
        end

        # Deregisters a deployment task
        def deregister(deployment_task)
            removed_streams = Array.new
            deployment_task.model.each_stream_mapping do |s, _|
                set = stream_to_deployment[s]
                set.delete(deployment_task)
                if set.empty?
                    stream_to_deployment.delete(s)
                    removed_streams << s
                end
            end

            # Remove the streams, and make sure that if the aligner read one
            # sample, that sample will still be available at the next step
            if stream_aligner.remove_streams(*removed_streams)
                stream_aligner.step_back
            end
        end

        # Process the next sample, and feed it to the relevant deployment(s)
        def step
            stream_index, time, sample = stream_aligner.step
            stream = stream_aligner.streams[stream_index]
            stream_to_deployment[stream].each do |task|
                task.process_sample(stream, time, sample)
            end
        end
    end
end

