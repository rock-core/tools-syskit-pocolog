module Syskit
    module Pocolog
        # A set of log streams
        class Streams
            # Load the set of streams available from a directory
            #
            # Note that in each directory, a stream's identity (task name,
            # port/property name and type) must be unique. If you need to mix
            # log streams, load files in separate {Streams} objects
            def self.from_dir(path)
                streams = new
                streams.add_dir(Pathname(path))
                streams
            end

            # Load the set of streams available from a file
            def self.from_file(file)
                streams = new
                streams.add_file(Pathname(file))
                streams
            end

            # The list of streams that are available
            attr_reader :streams

            def initialize(streams = Array.new)
                @streams = streams
            end

            def each_stream(&block)
                streams.each(&block)
            end

            # Load all log files from a directory
            def add_dir(path)
                path.children.each do |file_or_dir|
                    if file_or_dir.file? && file_or_dir.to_s =~ /\.\d+\.log$/
                        add_file(file_or_dir)
                    end
                end
            end

            # Load the streams from a log file
            def add_file(file)
                ::Pocolog::Logfiles.open(file).streams.each do |s|
                    add_stream(s)
                end
            end

            # Add a new stream
            #
            # @param [Pocolog::DataStream] s
            def add_stream(s)
                streams << s
            end

            # Find all streams whose metadata match the given query
            def find_all_streams(query)
                streams.find_all { |s| query === s }
            end

            # Find all streams that belong to a task
            def find_task_by_name(name)
                streams = find_all_streams(RockStreamMatcher.new.task_name(name))
                if !streams.empty?
                    TaskStreams.new(streams)
                end
            end

            # Give access to the streams per-task by calling <task_name>_task
            def method_missing(m, *args, &block)
                MetaRuby::DSLs.find_through_method_missing(
                    self, m, args, 'task' => "find_task_by_name") || super
            end
        end
    end
end

