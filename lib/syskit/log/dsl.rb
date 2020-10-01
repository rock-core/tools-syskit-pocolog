# frozen_string_literalr: true

require "syskit/log"
require "syskit/log/datastore"

module Syskit
    module Log
        # Toplevel module that defines a DSL for log manipulation in e.g. Jupyter
        # notebooks
        #
        # All the DSL methods are prefixed with `ds_`
        module DSL
            def self.extend_object(object)
                super
                object.__syskit_log_dsl_initialize
            end

            def __syskit_log_dsl_initialize
                @datastore = Datastore.default if Datastore.default_defined?
                @streams = {}
            end

            # Select a specific datastore
            #
            # By defautl, the default datastore is used (as defined by the
            # SYSKIT_LOG_STORE environment variable)
            def datastore_select(path)
                @datastore = Datastore.new(Pathname(path))
            end

            # The current datastore
            #
            # This is the default datastore unless {#datastore_select} has been
            # called
            attr_reader :datastore

            # The current dataset as selectedby {#dataset_select}
            attr_reader :dataset

            # The configured interval
            #
            # Sample enumeration performed through {#samples_of} (as for instance
            # in #to_daru_frame) will restrict themselves to this interval
            #
            # @return [nil,(Time,Time)]
            attr_reader :interval

            # Select the curent dataset
            #
            # @param [String,Hash] query if a string, this is interpreted as a
            #   dataset digest. Otherwise, it is used as a metadata query and
            #   given to {Datastore#find}
            def dataset_select(query = nil)
                return (@dataset = datastore.get(query)) if query.respond_to?(:to_str)

                matches =
                    if query
                        datastore.find_all(query)
                    else
                        datastore.each_dataset.to_a
                    end

                if matches.size == 1
                    @dataset = matches.first
                elsif matches.empty?
                    raise ArgumentError, "no dataset matches the given metadata"
                else
                    @dataset = __dataset_user_select(matches)
                end
            end

            def __dataset_user_select(candidates)
                this = self
                candidates = candidates.each_with_object({}) do |dataset, h|
                    format = this.__dataset_format(dataset)
                    h[format] = dataset
                end

                result = IRuby.form do
                    radio(:selected_dataset, *candidates.keys)
                    button
                end
                candidates.fetch(result[:selected_dataset])
            end

            def __dataset_format(dataset)
                description = dataset.metadata_fetch_all(
                    "description", "<no description>"
                )
                digest = @datastore.short_digest(dataset)
                format = "% #{digest.size}s"
                description = description
                              .zip([digest])
                              .map { |a, b| "#{format % [b]} #{a}" }
                              .join

                metadata = dataset.metadata.map do |k, v|
                    next if k == "description"

                    "#{k}: #{v.to_a.sort.join(', ')}"
                end.compact.join("; ")

                "#{description} - #{metadata}"
            end

            # Select the time interval that will be processed
            def interval_select(start_time, end_time, reset_zero: true)
                @base_interval = [start_time, end_time]
                interval_reset(reset_zero: reset_zero)
            end

            # Select the time interval that will be processed using the start
            # and end time of a stream
            def interval_select_from_stream(stream, reset_zero: true)
                @base_interval = stream.interval_lg
                interval_reset(reset_zero: reset_zero)
            end

            # Pick the zero time
            def interval_select_zero_time(time)
                @interval_zero_time = time
            end

            # Reset the interval to the last interval selected by
            # {#interval_select_from_stream}
            def interval_reset(reset_zero: true)
                @interval = @base_interval.dup
                @interval_zero_time = @interval[0] if reset_zero
            end

            # The start of the interval
            #
            # @return [Time,nil]
            def interval_start
                @interval[0]
            end

            # The end of the interval
            #
            # @return [Time,nil]
            def interval_end
                @interval[1]
            end

            # The zero time
            attr_reader :interval_zero_time

            # Shift the interval start by that many seconds (fractional),
            # starting at the start of the main selected interval
            #
            # @param [Float] offset
            def interval_shift_start(offset)
                @interval[0] += offset
            end

            # Set the interval end to that many seconds after the current start
            #
            # @param [Float] offset
            def interval_shift_end(offset)
                @interval[1] = @interval[0] + offset
            end

            # Convert fields of a data stream into a Daru frame
            def to_daru_frame(stream)
                samples = samples_of(stream)
                builder = Daru::FrameBuilder.new(stream.type)
                yield(builder)
                builder.to_daru_frame(@interval_zero_time, samples)
            end

            # Resolve a sample enumerator from a stream object
            def samples_of(stream)
                return stream if stream.kind_of?(Pocolog::SampleEnumerator)

                samples = stream.samples
                return samples unless @interval

                samples.from(@interval[0]).to(@interval[1])
            end

            # @api private
            #
            # Find the streams originating from the same task, by its deployed name
            #
            # @return [nil,TaskStreams]
            def __find_task_by_name(name)
                return unless @dataset

                @dataset.streams.find_task_by_name(name)
            end

            def respond_to_missing?(m, include_private = false)
                MetaRuby::DSLs.has_through_method_missing?(
                    self, m,
                    "_task" => "__find_task_by_name"
                ) || super
            end

            # Give access to the streams per-task by calling <task_name>_task
            def method_missing(m, *args, &block)
                MetaRuby::DSLs.find_through_method_missing(
                    self, m, args,
                    "_task" => "__find_task_by_name"
                ) || super
            end
        end
    end
end
