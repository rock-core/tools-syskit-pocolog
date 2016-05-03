module Syskit::Pocolog
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

        # The common registry
        attr_reader :registry

        def initialize(streams = Array.new)
            @streams = streams
            @registry = Typelib::Registry.new
        end

        def each_stream(&block)
            streams.each(&block)
        end

        # @api private
        #
        # Find the pocolog logfile groups and returns them
        #
        # @param [Pathname] path the directory to look into
        # @return [Array<Array<Pathname>>]
        def make_file_groups_in_dir(path)
            files_per_basename = Hash.new { |h, k| h[k] = Array.new }
            path.children.each do |file_or_dir|
                next if !file_or_dir.file?
                next if !(file_or_dir.extname == '.log')

                base_filename = file_or_dir.sub_ext('')
                id = base_filename.extname[1..-1]
                next if id !~ /^\d+$/
                base_filename = base_filename.sub_ext('')
                files_per_basename[base_filename.to_s][Integer(id)] = file_or_dir
            end
            files_per_basename.values.map do |list|
                list.compact
            end
        end

        # Load all log files from a directory
        def add_dir(path)
            make_file_groups_in_dir(path).each do |files|
                add_file_group(files)
            end
        end

        # Open a list of pocolog files that belong as a group
        #
        # I.e. each file is part of the same general datastream
        #
        # @raise Errno::ENOENT if the path does not exist
        def add_file_group(group)
            file = Pocolog::Logfiles.new(*group.map { |path| path.open }, registry)
            file.streams.each do |s|
                add_stream(s)
            end
        end

        # Load the streams from a log file
        def add_file(file)
            add_file_group([file])
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

