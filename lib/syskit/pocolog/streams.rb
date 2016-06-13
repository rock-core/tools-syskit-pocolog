module Syskit::Pocolog
    # A set of log streams
    class Streams
        # Basename for the metadata file generated by 'syskit pocolog normalize'
        METADATA_BASENAME = 'syskit-dataset.yml'

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

        def initialize(streams = Array.new, registry: Typelib::Registry.new)
            @streams = streams
            @registry = registry
        end

        # The number of data streams in self
        def num_streams
            streams.size
        end

        # Enumerate the streams by grouping them per-task
        #
        # It will only enumerate the tasks that are "functional", that is that
        # they have a name and model, and the model can be resolved
        #
        # @param [Boolean] load_models whether the method should attempt to
        #   load the missing models
        # @param [#using_task_library] app the app that should be used to
        #   load the missing models when load_models is true
        # @yieldparam [TaskStreams] task
        def each_task(load_models: true, ignore_missing_task_models: true, loader: Roby.app.default_loader)
            return enum_for(__method__, load_models: load_models, ignore_missing_task_models: ignore_missing_task_models, loader: loader) if !block_given?

            available_tasks = Hash.new { |h, k| h[k] = Array.new }
            ignored_streams = Hash.new { |h, k| h[k] = Array.new }
            empty_task_models = Array.new
            each_stream do |s|
                if !(task_model_name = s.metadata['rock_task_model'])
                    next
                elsif task_model_name.empty?
                    empty_task_models << s
                    next
                end

                task_m = Syskit::TaskContext.find_model_from_orogen_name(task_model_name)
                if !task_m && load_models 
                    orogen_project_name, *_tail = task_model_name.split('::')
                    loader.project_model_from_name(orogen_project_name)
                    begin
                        task_m = Syskit::TaskContext.find_model_from_orogen_name(task_model_name)
                    rescue NotFound
                        raise if !ignore_missing_task_models
                    end
                end
                if task_m
                    available_tasks[s.metadata['rock_task_name']] << s
                else
                    ignored_streams[task_model_name] << s
                end
            end

            if !empty_task_models.empty?
                Syskit::Pocolog.warn "ignored #{empty_task_models.size} streams that declared a task model, but left it empty: #{empty_task_models.map(&:name).sort.join(", ")}"
            end

            ignored_streams.each do |task_model_name, streams|
                Syskit::Pocolog.warn "ignored #{streams.size} streams because the task model #{task_model_name.inspect} cannot be found: #{streams.map(&:name).sort.join(", ")}"
            end

            available_tasks.each_value.map do |streams|
                yield(TaskStreams.new(streams))
            end
        end
        
        # Enumerate the streams
        #
        # @yieldparam [Pocolog::DataStream]
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
            metadata_path = (path + METADATA_BASENAME)
            if metadata_path.exist?
                add_normalized_dataset(metadata_path)
            else
                make_file_groups_in_dir(path).each do |files|
                    add_file_group(files)
                end
            end
        end

        # Load a dataset that has been normalized with 'syskit pocolog normalize'
        #
        # Metadata entries contains
        #
        # name:: the stream name
        # path:: the stream's backing pocolog file. Normalized datasets always
        #   have one file per stream
        # file_mtime:: the stream's backing pocolog file's modification time. This is
        #   used as a sanity check when loading the metadata
        # file_size:: the stream's backing pocolog file's size. . This is
        #   used as a sanity check when loading the metadata
        # registry_sha256:: each pocolog file has a separate file that contains
        #   the stream's typelib registry marshalled as XML. This is the
        #   registry's checksum. Only used for consistency checks.
        # type:: the stream's typename
        # interval_rt:: the stream's realtime interval
        # interval_lg:: the stream's logical time interval
        # stream_size:: the stream's size in samples
        # metadata:: the stream's metadata (as a hash)
        def add_normalized_dataset(metadata_path)
            info = YAML.load(metadata_path.read)
            info.each do |stream|
                stream_name = stream['name']
                stream_path = Pathname.new(stream['path'])
                stream_tlb = stream_path.sub_ext('.tlb')
                if !stream_path.exist?
                    raise InvalidNormalizedDataset.new(stream),
                        "#{stream_name}'s backing file #{stream_path} does not exist anymore, re-run syskit pocolog normalize"
                elsif !stream_tlb.exist?
                    raise InvalidNormalizedDataset.new(stream),
                        "#{stream_name}'s registry file #{stream_tlb} does not exist anymore, re-run syskit pocolog normalize"
                elsif stream['file_mtime'] != stream_path.stat.mtime
                    raise InvalidNormalizedDataset.new(stream),
                        "#{stream_name}'s backing file #{stream_path} modification time has changed since the last run of syskit pocolog normalize, re-run to fix"
                elsif stream['file_size'] != stream_path.stat.size
                    raise InvalidNormalizedDataset.new(stream),
                        "#{stream_name}'s backing file #{stream_path} size has changed since the last run of syskit pocolog normalize, re-run to fix"
                else
                    registry_xml = stream_tlb.read
                    checksum = Digest::SHA256.base64digest(registry_xml)
                    if stream['registry_sha256'] != checksum
                        raise InvalidNormalizedDataset.new(stream),
                            "#{stream_name}'s registry file #{stream_tlb} checksum has changed since the last run of syskit pocolog normalize, re-run to fix"
                    end

                    begin
                        registry = Typelib::Registry.from_xml(stream_tlb.read)
                    rescue Exception => e
                        raise e, "#{stream_name}'s registry file #{stream_tlb} cannot be loaded: #{e.message}", e.backtrace
                    end

                    stream = LazyDataStream.new(
                        stream_path, stream_name, registry.get(stream['type']),
                        stream['interval_rt'], stream['interval_lg'], stream['stream_size'],
                        stream['metadata'])
                    add_stream(stream)
                end
            end
        end

        # @api private
        #
        # Update the metadata information stored within a given path
        def self.update_normalized_metadata(path)
            metadata_path = path + Streams::METADATA_BASENAME
            if metadata_path.exist?
                metadata = YAML.load(metadata_path.read)
            else
                metadata = Array.new
            end
            yield(metadata)
            metadata_path.open('w') do |io|
                YAML.dump(metadata, io)
            end
        end

        # @api private
        #
        # Save a stream's registry in a normalized dataset, and returns the
        # registry's checksum
        #
        # @param [Pathname] stream_path the path to the stream's backing file
        # @param [Pocolog::DataStream] stream the stream
        # @return [String] the registry's checksum
        def self.save_registry_in_normalized_dataset(stream_path, stream)
            stream_tlb = stream_path.sub_ext('.tlb')
            if stream_tlb == stream_path
                raise ArgumentError, "cannot save the stream registry in #{stream_tlb}, it would overwrite the stream itself"
            end
            registry_xml = stream.type.to_xml
            stream_tlb.open('w') do |io|
                io.write registry_xml
            end
            Digest::SHA256.base64digest(registry_xml)
        end

        # @api private
        #
        # Create an entry suitable for marshalling in the metadata file for a
        # given stream
        #
        # @param [Pathname] stream_path the path to the stream's backing file
        # @param [Pocolog::DataStream] stream the stream
        # @return [Hash]
        def self.create_metadata_entry(stream_path, stream, registry_checksum)
            entry = Hash.new
            entry['path'] = stream_path.realpath.to_s
            entry['file_size'] = stream_path.stat.size
            entry['file_mtime'] = stream_path.stat.mtime
            entry['registry_sha256'] = registry_checksum

            entry['name'] = stream.name
            entry['type'] = stream.type.name
            entry['interval_rt'] = stream.time_interval(true)
            entry['interval_lg'] = stream.time_interval(false)
            entry['stream_size'] = stream.size
            entry['metadata'] = stream.metadata
            entry
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

        def sanitize_metadata(stream)
            if (model = stream.metadata['rock_task_model']) && model.empty?
                Syskit::Pocolog.warn "removing empty metadata property 'rock_task_model' from #{stream.name}"
                stream.metadata.delete('rock_task_model')
            end
            if task_name = stream.metadata['rock_task_name']
                stream.metadata['rock_task_name'] = task_name.gsub(/.*\//, '')
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
            sanitize_metadata(s)
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

        # Creates a deployment group object that deploys all streams
        def to_deployment_group(load_models: true, loader: Roby.app.default_loader, ignore_missing_task_models: true)
            group = Syskit::Models::DeploymentGroup.new
            each_task(load_models: load_models, loader: loader, ignore_missing_task_models: ignore_missing_task_models) do |task_streams|
                group.use_pocolog_task(task_streams)
            end
            group
        end
    end
end

