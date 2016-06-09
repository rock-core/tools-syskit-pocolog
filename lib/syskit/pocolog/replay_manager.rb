module Syskit::Pocolog
    # The object that manages the replay itself
    #
    # Deployments register and deregister themselves when started/stopped.
    #
    # There is one per execution engine, which is accessible via
    # {Extensions::ExecutionEngine#pocolog_replay_manager}. The way to add/remove
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

        # The current logical time
        attr_reader :time

        # The execution engine we should use to run
        attr_reader :execution_engine

        # @api private
        #
        # The realtime base for {#process_in_realtime}
        #
        # The realtime play process basically works by assuming that
        # base_real_time == {#base_logical_time}
        attr_reader :base_real_time

        # @api private
        #
        # The logical time base for {#process_in_realtime}
        #
        # The realtime play process basically works by assuming that
        # base_real_time == {#base_logical_time}
        attr_reader :base_logical_time

        def initialize(execution_engine)
            @execution_engine = execution_engine
            @handler_id = nil
            @stream_aligner = Pocolog::StreamAligner.new(false)
            @stream_to_deployment = Hash.new { |h, k| h[k] = Set.new }
        end

        # Time of the first sample in the aligner
        def start_time
            stream_aligner.time_interval[0]
        end

        # Time of the last sample in the aligner
        def end_time
            stream_aligner.time_interval[1]
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
            if stream_aligner.add_streams(*new_streams)
                _, @time = stream_aligner.step_back
            else
                reset_replay_base_times
                if stream_aligner.eof?
                    @time = end_time
                else
                    @time = start_time
                end
            end
            reset_replay_base_times
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
                _, @time = stream_aligner.step_back
            else
                if stream_aligner.eof?
                    @time = end_time
                else
                    @time = start_time
                end
            end
            reset_replay_base_times
        end

        # Seek to the given time or sample index
        def seek(time_or_index)
            stream_index, time = stream_aligner.seek(time_or_index, false)
            if stream_index
                dispatch(stream_index, time)
                reset_replay_base_times
            end
        end

        # Process the next sample, and feed it to the relevant deployment(s)
        def step
            stream_index, time = stream_aligner.step
            if stream_index
                dispatch(stream_index, time)
            end
        end

        # Whether we're doing realtime replay
        def running?
            !!@handler_id
        end

        # Start replaying in realtime
        def start(replay_speed: 1)
            if running?
                raise RuntimeError, "already running"
            end
            reset_replay_base_times
            @handler_id = execution_engine.add_side_work_handler(description: "syskit-pocolog replay handler for #{self}") do
                process_in_realtime(replay_speed)
            end
        end

        def stop
            if !running?
                raise RuntimeError, "not running"
            end
            execution_engine.remove_side_work_handler(@handler_id)
            @handler_id = nil
        end

        # The minimum amount of time we should call sleep() for. Under that,
        # {#process_in_realtime} will not call sleep and just play some samples
        # slightly in advance
        MIN_TIME_DIFF_TO_SLEEP = 0.01

        # @api private
        #
        # Returns the end of the current engine cycle
        #
        # @return [Time]
        def end_of_current_engine_cycle
            execution_engine.cycle_start + execution_engine.cycle_length
        end

        # @api private
        #
        # Play samples required by the current execution engine's time
        def process_in_realtime(replay_speed, limit_real_time: end_of_current_engine_cycle)
            limit_logical_time = base_logical_time + (limit_real_time - base_real_time) * replay_speed

            while true
                stream_index, time = stream_aligner.step
                if !stream_index
                    return false
                elsif time > limit_logical_time
                    stream_aligner.step_back
                    return true
                end

                target_real_time = base_real_time + (time - base_logical_time) / replay_speed
                time_diff = target_real_time - Time.now
                if time_diff > MIN_TIME_DIFF_TO_SLEEP
                    sleep(time_diff)
                end
                dispatch(stream_index, time)
            end
        end


        # @api private
        #
        # Immediately dispatch the sample last read by {#stream_aligner}
        def dispatch(stream_index, time)
            @time = time
            stream = stream_aligner.streams[stream_index]
            tasks = stream_to_deployment[stream]
            if !tasks.empty?
                sample = stream_aligner.single_data(stream_index)
                tasks.each do |task|
                    task.process_sample(stream, time, sample)
                end
            end
        end

        # @api private
        #
        # Resets the reference times used to manage the realtime replay
        def reset_replay_base_times
            @base_real_time = Time.now
            @base_logical_time = time || start_time
        end
    end
end

